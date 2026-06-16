## Shared types, errors and the destructor for `AsyncFile`.
##
## Split out of `chronos_file` so the POSIX implementation submodules
## (`posix_backend`/`posix_handle`/`posix_io`/`posix_flush_close`) can all import the
## type and its fields. The object fields are exported (`*`) for that reason, so
## they are technically reachable by callers — treat them as internal.

import pkg/chronos
import pkg/chronos/[osutils, oserrno]

{.push raises: [].}

type
  AsyncFileObj* = object
    fd*: AsyncFD
    offset*: int64
      ## Logical file offset and the single source of truth for seekable files
      ## (regular files / block devices), where read/write use `pread`/`pwrite` at
      ## this offset and advance it. For non-seekable fds (pipe/FIFO/tty) the
      ## kernel position is used instead and this only tracks bytes consumed.
    seekable*: bool
      ## True for regular files / block devices: I/O goes through `pread`/`pwrite`
      ## at `offset`, leaving the kernel position untouched. False for
      ## pipe/FIFO/tty, which use sequential `read`/`write` + EAGAIN/`addReader2`.
    pushback*: seq[byte]
      ## Bytes read from the descriptor but logically "un-read" (currently only by
      ## `readLine` after a bare CR on a non-seekable fd). `read` serves these
      ## before issuing a syscall; `readBuffer` (low-level) bypasses them. The
      ## logical file position is `offset - pushback.len`.
    readFut*: Future[int]
      ## In-flight `readBuffer` future while it waits on `addReader2` (pipe/FIFO
      ## EAGAIN path). Tracked so `close` can fail it instead of leaking it.
    writeFut*: Future[void]
      ## In-flight `writeBuffer` future while it waits on `addWriter2`.
    appendMode*: bool
      ## True when opened with `fmAppend`. Append-mode writes use a sequential
      ## `write` (the kernel appends atomically) instead of `pwrite` at
      ## `offset`: POSIX specifies that `pwrite` honours its offset even under
      ## `O_APPEND`, so on conforming platforms (macOS/BSD) it would overwrite
      ## at a stale offset rather than append — only Linux deviates and appends
      ## regardless (a documented quirk). Positioned writes (`writeAt`/
      ## `writeBufferAt`) are rejected: on Linux the kernel would silently
      ## append instead of writing at the requested offset, and allowing them
      ## only off-Linux would make behavior platform-dependent.
    closed*: bool
      ## Set by `close` so a second `close` is a no-op (avoids closing an fd that
      ## may already have been reused).
    closing*: bool
      ## Set at the start of `closeWait`, before it suspends to cancel and drain
      ## in-flight ops. New operations are rejected from that point on, closing
      ## the window in which another task could slip a fresh read/write onto a
      ## descriptor that is about to be closed (such an op would otherwise fail
      ## with EBADF instead of the graceful contract closeWait promises).
    opened*: bool
      ## Set true only once a real constructor (`openAsync`/`newAsyncFile`) has
      ## taken ownership of a valid fd. The zero value is false, so a
      ## default-constructed `AsyncFile()` is inert: its destructor releases
      ## nothing (it owns no fd — fd 0 would otherwise be stdin) and every public
      ## operation is rejected. Without this, dropping such a handle would close
      ## stdin or abort in the dispatcher (unregistering an fd that was never
      ## registered).
    rbuf*: seq[byte]
      ## Seekable `readLine` read-ahead: bytes pread from the file but not yet
      ## logically consumed; `rbuf[rpos ..< rbuf.len]` are the unconsumed bytes.
      ## Lets successive `readLine`s share one pread instead of re-reading the
      ## post-terminator tail. Only ever populated for seekable fds (non-seekable
      ## `readLine` streams a byte at a time via `pushback`), so `rbuf` and
      ## `pushback` are never both non-empty. The logical read position is
      ## `offset - (rbuf.len - rpos) - pushback.len`. Dropped (with `offset`
      ## rewound) by `reconcile` before any offset-based read/write.
    rpos*: int ## Index of the next unconsumed byte in `rbuf` (see `rbuf`).

  AsyncFile* = ref AsyncFileObj
    ## Handle to a file opened for asynchronous I/O (`openAsync`/`newAsyncFile`).
    ## Call `close` (synchronous) or `closeWait` (asynchronous) when done. As a
    ## last-resort safety net a destructor closes the descriptor if it was never
    ## closed, but relying on that is discouraged — close explicitly.

  AsyncFileError* = object of AsyncError ## Base error for asynchronous file I/O.
  AsyncFileOsError* = object of AsyncFileError
    ## OS-level file I/O error carrying the originating `OSErrorCode`.
    code*: OSErrorCode

  AsyncFileBusyError* = object of AsyncFileError
    ## Raised when a second read/write is issued on a non-seekable fd
    ## (pipe/FIFO/tty) while one is already in flight. Only one read and one
    ## write may wait on the descriptor at a time. Subtype of `AsyncFileError`,
    ## so `except AsyncFileError` still catches it.

  AsyncFileIncompleteError* = object of AsyncFileError
    ## Raised by `readExactly` when end of file is reached before the requested
    ## number of bytes could be read. Subtype of `AsyncFileError`, so
    ## `except AsyncFileError` still catches it.

  AsyncFileLimitError* = object of AsyncFileError
    ## Raised by `readLine` when a line exceeds the caller-supplied `limit`
    ## before a terminator is found. Subtype of `AsyncFileError`, so
    ## `except AsyncFileError` still catches it.

  FlushKind* = enum
    ## Selects what `flush` asks the kernel to make durable.
    flushFull ## `fsync`: file data and metadata.
    flushDataOnly
      ## `fdatasync`: file data only (skips metadata-only updates such as
      ## mtime). Falls back to `fsync` on platforms without `fdatasync`
      ## (e.g. macOS), which subsumes its guarantees.

proc newAsyncFileOsError*(code: OSErrorCode, context = ""): ref AsyncFileOsError =
  let detail = "(" & $int(code) & ") " & osErrorMsg(code)
  let msg =
    if context.len > 0:
      context & ": " & detail
    else:
      detail
  (ref AsyncFileOsError)(code: code, msg: msg)

proc newAsyncFileError*(msg: string): ref AsyncFileError =
  (ref AsyncFileError)(msg: msg)

proc newAsyncFileBusyError*(msg: string): ref AsyncFileBusyError =
  (ref AsyncFileBusyError)(msg: msg)

proc newAsyncFileIncompleteError*(msg: string): ref AsyncFileIncompleteError =
  (ref AsyncFileIncompleteError)(msg: msg)

proc newAsyncFileLimitError*(msg: string): ref AsyncFileLimitError =
  (ref AsyncFileLimitError)(msg: msg)

proc `=destroy`(f: AsyncFileObj) =
  ## Best-effort safety net for a forgotten `close`: release the descriptor
  ## (and, for non-seekable fds, its dispatcher registration). Prefer an
  ## explicit `close`/`closeWait`; do not rely on this. While a pipe/FIFO
  ## read/write is in flight the dispatcher holds the readiness callback (which
  ## captures this handle), so the handle stays reachable and this destructor
  ## will not run until the op settles — but do not depend on that ordering:
  ## close such handles explicitly with `closeWait`, which cancels and drains
  ## in-flight ops first. The destructor itself neither fails nor cancels a
  ## pending future, so were it ever to run with one outstanding, a queued
  ## readiness callback could touch a freed descriptor/buffer.
  ##
  ## The dispatcher registration is thread-local state, so this safety net is
  ## only safe when the destructor runs on the thread that owns the dispatcher
  ## the fd was registered with, while that dispatcher is still alive. A
  ## handle whose last reference is dropped on another thread, or after the
  ## event loop has shut down, is outside that window — one more reason to
  ## close explicitly instead of relying on this.
  ##
  ## Only acts on a handle that a real constructor opened: a
  ## default-constructed `AsyncFile()` has `opened == false` and owns no fd, so
  ## it must not touch fd 0 (stdin) or unregister an fd that was never
  ## registered (which would abort in the dispatcher).
  if f.opened and not f.closed:
    if not f.seekable:
      discard unregister2(f.fd)
    discard closeFd(cint(f.fd))
  # A custom `=destroy` suppresses the compiler's automatic field destruction,
  # so the GC-managed fields must be released by hand or their refs/seq would
  # leak. The `Future` destructor is inferred as possibly-raising, but a
  # destructor must not raise, so the effect is cast away (it never raises in
  # practice).
  {.cast(raises: []).}:
    `=destroy`(f.pushback)
    `=destroy`(f.rbuf)
    `=destroy`(f.readFut)
    `=destroy`(f.writeFut)
