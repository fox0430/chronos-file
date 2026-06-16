## Asynchronous file I/O for chronos.
##
## Provides an `AsyncFile` type with a `std/asyncfile`-inspired (but not
## compatible) API that runs on the chronos event loop / `Future`. Notable
## differences: reads return `seq[byte]`, `readLine` returns `Opt[string]`
## (so an empty line is distinguishable from EOF), and one-shot helpers
## (`readFileAsync`/`writeFileAsync`/`withAsyncFile`) are provided.
##
## POSIX backend: each read/write issues the syscall immediately and only falls
## back to `addReader2`/`addWriter2` on `EAGAIN`/`EWOULDBLOCK`. Regular files never
## return `EAGAIN` (epoll cannot watch them), so they complete synchronously inside
## the `Future` — exactly mirroring `std/asyncfile`. Pipes/FIFOs/ttys take the
## `addReader2`/`addWriter2` path and are truly asynchronous.
##
## **Warning:** because regular-file reads/writes are synchronous syscalls wrapped
## in a `Future`, they block the whole chronos event loop while the kernel serves
## them. On slow or contended storage (network filesystems, loaded disks) this
## stalls every other async task. This matches `std/asyncfile`; true async for
## regular files would need io_uring or a thread pool.
##
## This module is POSIX-only for now. Windows (IOCP overlapped I/O) is not
## implemented yet; see the `when defined(windows)` block below for a sketch of
## the planned design.
##
## The implementation is split across `chronos_file/` for navigability: `common`
## (types/errors/destructor), `posix_backend` (raw-fd syscall wrappers),
## `posix_handle` (open/positioning/lifecycle), `posix_io` (the read/write surface)
## and `posix_flush_close` (flush, close, one-shot helpers). This module re-exports
## the public API of those submodules.

when defined(windows):
  # The Windows backend (IOCP overlapped I/O) is not implemented in this phase.
  # Planned design:
  #   - openAsync via createFile(FILE_FLAG_OVERLAPPED), register2 (= IOCP attach).
  #   - read/write issue overlapped readFile/writeFile with a RefCustomOverlapped
  #     whose offset/offsetHigh carry f.offset; GC_ref until the completion cb.
  #   - completion dispatched by poll() -> cb advances f.offset and completes;
  #     GC_unref unconditionally (chronos poll() does not GC_unref overlapped).
  #   - cancellation uses CancelIoEx (safe side) so pending I/O aborts before the
  #     caller-owned buffer of readBuffer/writeBuffer can be freed.
  {.
    error:
      "chronos_file: Windows (IOCP) backend not implemented yet; " &
      "POSIX only in this phase"
  .}
else:
  import std/syncio
  from std/os import FilePermission
  import pkg/chronos
  import pkg/chronos/oserrno

  import chronos_file/[common, posix_handle, posix_io, posix_flush_close]

  export chronos
  export oserrno
  export syncio.FileMode
  export os.FilePermission

  export
    common.AsyncFile, common.AsyncFileObj, common.AsyncFileError,
    common.AsyncFileOsError, common.AsyncFileBusyError, common.AsyncFileIncompleteError,
    common.AsyncFileLimitError, common.FlushKind

  export
    posix_handle.getFileSize, posix_handle.getFilePos, posix_handle.setFilePos,
    posix_handle.setFileSize, posix_handle.newAsyncFile, posix_handle.openAsync,
    posix_handle.isOpen, posix_handle.isClosed
  export posix_io
  export posix_flush_close
