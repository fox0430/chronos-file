## POSIX syscall wrappers that operate on a raw file descriptor, with no
## knowledge of `AsyncFile`. Imported by `posix_handle`/`posix_io`.

import std/[posix, syncio]
from std/os import FilePermission

import pkg/chronos/[osutils, oserrno]

import common

{.push raises: [].}

# Request large file support (64-bit off_t) on 32-bit targets so files larger
# than 2 GiB don't silently truncate offsets. This must reach the C
# preprocessor *before* the libc headers, so it goes through the compiler
# command line; a Nim `-d:_FILE_OFFSET_BITS=64` symbol would never become a C
# macro and would have no effect. Harmless on 64-bit targets, where off_t is
# already 64-bit.
{.passC: "-D_FILE_OFFSET_BITS=64".}

when sizeof(Off) < 8:
  {.
    warning:
      "chronos_file: off_t is still smaller than 64-bit despite requesting " &
      "large file support; this target cannot address files larger than 2 GiB " &
      "and offsets will silently truncate."
  .}

when defined(linux):
  let BLKGETSIZE64 {.importc, header: "<linux/fs.h>".}: culong
elif defined(macosx):
  let DKIOCGETBLOCKSIZE {.importc, header: "<sys/disk.h>".}: culong
  let DKIOCGETBLOCKCOUNT {.importc, header: "<sys/disk.h>".}: culong

proc toPosixFlags*(mode: FileMode): cint =
  case mode
  of fmRead:
    O_RDONLY
  of fmWrite:
    O_WRONLY or O_CREAT or O_TRUNC
  of fmReadWrite:
    O_RDWR or O_CREAT or O_TRUNC
  of fmReadWriteExisting:
    O_RDWR
  of fmAppend:
    O_WRONLY or O_CREAT or O_APPEND

proc toPosixMode*(perm: set[FilePermission]): cint =
  if fpUserRead in perm:
    result = result or S_IRUSR
  if fpUserWrite in perm:
    result = result or S_IWUSR
  if fpUserExec in perm:
    result = result or S_IXUSR
  if fpGroupRead in perm:
    result = result or S_IRGRP
  if fpGroupWrite in perm:
    result = result or S_IWGRP
  if fpGroupExec in perm:
    result = result or S_IXGRP
  if fpOthersRead in perm:
    result = result or S_IROTH
  if fpOthersWrite in perm:
    result = result or S_IWOTH
  if fpOthersExec in perm:
    result = result or S_IXOTH

proc isSeekable*(fd: cint): bool {.raises: [AsyncFileError].} =
  ## A regular file or block device supports `pread`/`pwrite` at an explicit
  ## offset; pipes/FIFOs/ttys do not (they return ESPIPE).
  var st: Stat
  if handleEintr(fstat(fd, st)) == -1:
    raise newAsyncFileOsError(osLastError(), "fstat")
  S_ISREG(st.st_mode) or S_ISBLK(st.st_mode)

proc doPread*(
    fd: cint, buf: pointer, size: int, off: int64, context = ""
): int {.raises: [AsyncFileError].} =
  ## Single `pread` (retried on EINTR); returns bytes read (0 = EOF). Raises on
  ## error, prefixing the message with `context` (the calling operation's name).
  let res = handleEintr(pread(fd, buf, size, Off(off)))
  if res < 0:
    raise newAsyncFileOsError(osLastError(), context)
  res

proc doPwrite*(
    fd: cint, buf: pointer, size: int, off: int64, context = ""
): int {.raises: [AsyncFileError].} =
  ## Single `pwrite` (retried on EINTR); returns bytes written (> 0). Raises on
  ## error, treating a 0-byte write of a non-empty request as `EIO` and
  ## prefixing the message with `context` (the calling operation's name).
  let res = handleEintr(pwrite(fd, buf, size, Off(off)))
  if res < 0:
    raise newAsyncFileOsError(osLastError(), context)
  elif res == 0:
    raise newAsyncFileOsError(oserrno.EIO, context)
  res

proc doWrite*(
    fd: cint, buf: pointer, size: int, context = ""
): int {.raises: [AsyncFileError].} =
  ## Single sequential `write` (retried on EINTR); returns bytes written
  ## (> 0). Used for append-mode files, where the kernel must pick the write
  ## position (atomic append to the current end of file): POSIX specifies
  ## that `pwrite` honours its explicit offset even under `O_APPEND`, so on
  ## conforming platforms (macOS/BSD) `pwrite` at the tracked offset would
  ## overwrite instead of append — only Linux deviates and appends regardless
  ## (a documented quirk, see BUGS in pwrite(2)). Raises on error, treating a
  ## 0-byte write of a non-empty request as `EIO` and prefixing the message
  ## with `context` (the calling operation's name).
  let res = handleEintr(posix.write(fd, buf, size))
  if res < 0:
    raise newAsyncFileOsError(osLastError(), context)
  elif res == 0:
    raise newAsyncFileOsError(oserrno.EIO, context)
  res

proc blockDeviceSize*(fd: cint): int64 {.raises: [AsyncFileError].} =
  ## Real byte size of a block device. `fstat` reports `st_size == 0` for block
  ## devices, so query the kernel via `ioctl`. On platforms without a known
  ## ioctl this returns 0 (same as the old behaviour).
  when defined(linux):
    var nbytes: uint64 = 0
    if handleEintr(ioctl(fd, uint(BLKGETSIZE64), addr nbytes)) == -1:
      raise newAsyncFileOsError(osLastError(), "ioctl(BLKGETSIZE64)")
    int64(nbytes)
  elif defined(macosx):
    var blockSize: uint32 = 0
    var blockCount: uint64 = 0
    if handleEintr(ioctl(fd, uint(DKIOCGETBLOCKSIZE), addr blockSize)) == -1:
      raise newAsyncFileOsError(osLastError(), "ioctl(DKIOCGETBLOCKSIZE)")
    if handleEintr(ioctl(fd, uint(DKIOCGETBLOCKCOUNT), addr blockCount)) == -1:
      raise newAsyncFileOsError(osLastError(), "ioctl(DKIOCGETBLOCKCOUNT)")
    int64(blockSize) * int64(blockCount)
  else:
    0'i64
