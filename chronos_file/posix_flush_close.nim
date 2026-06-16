## Durability (`flush` via a worker thread), teardown (`close`/`closeWait`) and
## the one-shot file helpers built on the rest of the API.

import std/[posix, typedthreads]
from std/os import FilePermission

import pkg/chronos
import pkg/chronos/[osutils, oserrno, threadsync]

import common, posix_handle, posix_io

{.push raises: [].}

type FlushCtx = object
  ## State shared between `flush` and its worker thread. Holds no GC-managed
  ## fields, so a `ptr` to it may cross threads. Owned by `flush` (it lives in
  ## the async proc's environment, which `noCancel` keeps alive to completion);
  ## the worker never touches it again after firing the signal.
  signal: ThreadSignalPtr
  fd: cint ## dup of the file's descriptor; closed by the worker when done.
  dataOnly: bool
  errCode: int32 ## errno of a failed sync, or 0 on success.

proc flushWorker(ctx: ptr FlushCtx) {.thread.} =
  ## Runs the blocking fsync/fdatasync off the event loop. Settles the ctx
  ## (errCode), returns the dup'd fd and fires the signal — strictly in that
  ## order, so when the loop side wakes up the result is final and no fd is
  ## left behind. Must not touch `ctx` after `fireSync`: from that point the
  ## loop side may already have freed it.
  let res =
    when defined(linux) or defined(android) or defined(freebsd) or defined(netbsd):
      if ctx.dataOnly:
        handleEintr(fdatasync(ctx.fd))
      else:
        handleEintr(fsync(ctx.fd))
    else:
      # macOS / OpenBSD and friends do not provide `fdatasync` (referencing it
      # is a C compile error there), so fall back to `fsync` regardless of
      # `dataOnly`.
      handleEintr(fsync(ctx.fd))
  ctx.errCode =
    if res == -1:
      int32(osLastError())
    else:
      0
  discard closeFd(ctx.fd)

  for _ in 0 ..< 3:
    # A lost signal would leave `flush` suspended forever — a Future cannot be
    # failed from a foreign thread, and the loop side waits unconditionally
    # (`noCancel`). `fireSync` already blocks internally on a full signal pipe,
    # so an error here is exceptional (EBADF-class); retry a few times as a
    # best-effort mitigation rather than giving up on the first failure. After
    # a successful fire `ctx` must not be touched again (the loop side may have
    # freed it) — the immediate `break` guarantees that.
    if ctx.signal.fireSync().isOk():
      break

proc flush*(
    f: AsyncFile, kind = flushFull
): Future[void] {.async: (raises: [AsyncFileError]).} =
  ## Flushes buffered file data to the storage device (`fsync`, or `fdatasync`
  ## when `kind == flushDataOnly`). The sync runs on a dedicated worker thread, so — unlike
  ## regular-file reads/writes — it does **not** block the event loop; other
  ## tasks keep running while the kernel writes back. Non-seekable fds
  ## (pipes/FIFOs/ttys) cannot be synced and fail with EINVAL — they are
  ## rejected up-front, before any worker thread is spawned.
  ##
  ## The worker syncs a `dup` of the descriptor (durability is a property of
  ## the file, not the descriptor), so calling `close`/`closeWait` while a
  ## flush is in flight is safe: the flush keeps running on its own descriptor
  ## and still reports its result.
  ##
  ## **Cancellation:** an in-progress `fsync` cannot be aborted, so cancelling
  ## this future does not detach it — cancellation is deferred (`noCancel`)
  ## and the future settles only once the sync finishes. `CancelledError` is
  ## therefore never raised.
  ##
  ## **Platform note:** `fdatasync` only exists on Linux/Android and the BSDs
  ## that provide it. On macOS (and other platforms lacking it) `flushDataOnly`
  ## is ignored and a plain `fsync` is issued instead — `fsync` subsumes
  ## `fdatasync`'s guarantees, so only the data-only optimization is lost.
  ## Also note that on macOS `fsync` does not force a physical platter flush
  ## (that needs `fcntl(F_FULLFSYNC)`), matching the rest of the POSIX world.
  checkOpen(f)

  if not f.seekable:
    # Only regular files and block devices (the seekable fds) can be fsync'd;
    # pipes/FIFOs/ttys return EINVAL. Reject them here so the heavy dup + signal
    # + worker-thread path is never spun up only to fail.
    raise newAsyncFileOsError(oserrno.EINVAL, "flush")

  let dupFd = dup(cint(f.fd))
  if dupFd == -1:
    raise newAsyncFileOsError(osLastError(), "flush")

  # Keep the worker's private fd out of child processes, mirroring openAsync's
  # O_CLOEXEC. Best-effort.
  discard setDescriptorInheritance(dupFd, false)
  let signal = ThreadSignalPtr.new().valueOr:
    discard closeFd(dupFd)
    raise newAsyncFileError("flush: cannot create completion signal: " & error)

  # `ctx` and `thr` live in this proc's environment; `noCancel` below
  # guarantees the proc runs to completion, so both outlive the worker.
  var ctx = FlushCtx(signal: signal, fd: dupFd, dataOnly: kind == flushDataOnly)
  var thr: Thread[ptr FlushCtx]
  try:
    createThread(thr, flushWorker, addr ctx)
  except CatchableError as e:
    discard closeFd(dupFd)
    discard signal.close()
    raise newAsyncFileError("flush: cannot create worker thread: " & e.msg)
  try:
    await noCancel(signal.wait())
  except AsyncError as e:
    raise newAsyncFileError("flush: wait on completion signal failed: " & e.msg)
  finally:
    # The worker fires the signal as its last action, so on the normal path it
    # is already exiting and the join takes microseconds. Only if the wait
    # itself failed can this block until the sync finishes (rare; accepted).
    joinThread(thr)
    # Closing the signal unregisters its event fd from this thread's
    # dispatcher, so it must happen here on the loop thread, not in the worker.
    discard signal.close()

  if ctx.errCode != 0:
    raise newAsyncFileOsError(OSErrorCode(ctx.errCode), "flush")

proc closeImpl(f: AsyncFile) {.raises: [AsyncFileError].} =
  ## Shared body of `close` and `closeWait`: fails any in-flight pipe/FIFO
  ## futures, unregisters non-seekable fds and closes the descriptor. Carries
  ## no `closing` guard, so `closeWait` can call it while `f.closing` is set
  ## (the public `close` adds that guard).
  if not f.opened or f.closed:
    return

  f.closed = true
  if not f.readFut.isNil and not f.readFut.finished():
    discard removeReader2(f.fd)
    let fut = f.readFut
    f.readFut = nil
    fut.fail(newAsyncFileOsError(oserrno.EBADF, "file closed while read pending"))
  if not f.writeFut.isNil and not f.writeFut.finished():
    discard removeWriter2(f.fd)
    let fut = f.writeFut
    f.writeFut = nil
    fut.fail(newAsyncFileOsError(oserrno.EBADF, "file closed while write pending"))

  # Seekable fds were never registered (they bypass the dispatcher), so only
  # non-seekable fds need unregistering. Mirrors openAsync / newAsyncFile.
  var unregErr = OSErrorCode(0)
  if not f.seekable:
    let ures = unregister2(f.fd)
    if ures.isErr:
      unregErr = ures.error
  let closeFailed = closeFd(cint(f.fd)) != 0
  let closeErr =
    if closeFailed:
      osLastError()
    else:
      OSErrorCode(0)

  # Raise the unregister error only when that was the sole failure. A close
  # failure always takes precedence and is never dropped, even if unregister
  # also failed (closing the fd is the more important outcome to report).
  if unregErr != OSErrorCode(0) and not closeFailed:
    raise newAsyncFileOsError(unregErr, "close (unregister)")
  if closeFailed:
    raise newAsyncFileOsError(closeErr, "close")

proc close*(f: AsyncFile) {.raises: [AsyncFileError].} =
  ## Unregisters and closes the file. The descriptor is always closed, even if
  ## unregistering from the dispatcher fails, so the fd cannot leak.
  ##
  ## If a `read`/`write` is still in flight (pipe/FIFO EAGAIN path), its future
  ## is failed rather than left pending forever; the caller should still avoid
  ## closing while operations are outstanding.
  ##
  ## Idempotent: a second `close` is a no-op, so the underlying fd (which may
  ## have been reused) is never closed twice. A `close` issued while a
  ## `closeWait` is draining is likewise a no-op: the file is about to be
  ## closed anyway, and failing the futures `closeWait` is cancelling would
  ## replace its graceful `CancelledError` contract with an `EBADF` error.
  ##
  ## The `closed` flag is set before the `close` syscall is attempted. `closeFd`
  ## retries `EINTR` internally (chronos wraps it in `handleEintr`); a non-`EINTR`
  ## failure is **not** retried by this proc — the fd is considered released and
  ## is not closed again (on Linux a failed `close` still releases the
  ## descriptor, so retrying could close an fd the OS has since reused).
  if f.closing:
    return
  closeImpl(f)

proc closeWait*(f: AsyncFile): Future[void] {.async: (raises: []).} =
  ## Asynchronous, graceful counterpart to `close`. Any in-flight pipe/FIFO
  ## read/write is *cancelled* (so its awaiter sees `CancelledError` rather than
  ## the `EBADF` failure that synchronous `close` injects) and awaited before the
  ## descriptor is closed.
  ##
  ## Idempotent: a second call (or a call after `close`) is a no-op. Note that
  ## a second `closeWait` issued while the first is still draining returns
  ## immediately, i.e. possibly before the descriptor is actually closed; a
  ## synchronous `close` issued during the drain is likewise a no-op (so it
  ## cannot replace the graceful cancellation with an `EBADF` failure). Never
  ## raises — a failure of the underlying `close` syscall is swallowed (the fd
  ## is released regardless), matching chronos' `closeWait` convention.
  ##
  ## From the moment `closeWait` is called, new operations on the file are
  ## rejected with `AsyncFileError` — the cancel-and-drain below suspends, and
  ## an operation slipped into that window would otherwise fail with `EBADF`
  ## instead of the graceful cancellation contract this proc promises.
  if not f.opened or f.closed or f.closing:
    return

  f.closing = true

  # Cancel and drain any pending reader/writer first. `cancelAndWait` runs the
  # registered cancel callback (which removes the reader/writer and clears the
  # tracking field), so by the time `close` runs there is nothing in flight.
  if not f.readFut.isNil and not f.readFut.finished():
    await f.readFut.cancelAndWait()
  if not f.writeFut.isNil and not f.writeFut.finished():
    await f.writeFut.cancelAndWait()
  try:
    # Call the shared body directly: the public `close` is a no-op while
    # `f.closing` is set (by design, to protect this drain from a concurrent
    # synchronous close), but here the drain is complete and the descriptor
    # must actually be closed.
    closeImpl(f)
  except AsyncFileError:
    discard

proc readFileAsync*(
    path: string
): Future[string] {.async: (raises: [AsyncFileError, CancelledError]).} =
  ## One-shot convenience: opens `path` read-only, reads the whole file as a
  ## `string` and closes it again (also on error/cancellation).
  let f = openAsync(path, fmRead)
  try:
    result = await f.readAllString()
  finally:
    await f.closeWait()

proc readFileBytesAsync*(
    path: string
): Future[seq[byte]] {.async: (raises: [AsyncFileError, CancelledError]).} =
  ## One-shot convenience: opens `path` read-only, reads the whole file as a
  ## `seq[byte]` and closes it again (also on error/cancellation).
  let f = openAsync(path, fmRead)
  try:
    result = await f.readAll()
  finally:
    await f.closeWait()

proc writeFileAsync*(
    path: string,
    data: seq[byte],
    perm: set[FilePermission] = {fpUserRead, fpUserWrite, fpGroupRead, fpOthersRead},
): Future[void] {.async: (raises: [AsyncFileError, CancelledError]).} =
  ## One-shot convenience: creates/truncates `path` (`fmWrite`, creation mode
  ## `perm`), writes `data` and closes it again (also on error/cancellation).
  let f = openAsync(path, fmWrite, perm)
  try:
    await f.write(data)
  finally:
    await f.closeWait()

proc writeFileAsync*(
    path: string,
    data: string,
    perm: set[FilePermission] = {fpUserRead, fpUserWrite, fpGroupRead, fpOthersRead},
): Future[void] {.async: (raises: [AsyncFileError, CancelledError]).} =
  ## One-shot convenience: creates/truncates `path` (`fmWrite`, creation mode
  ## `perm`), writes the string `data` and closes it again (also on
  ## error/cancellation).
  let f = openAsync(path, fmWrite, perm)
  try:
    await f.write(data)
  finally:
    await f.closeWait()

template withAsyncFile*(name: untyped, path: string, mode: FileMode, body: untyped) =
  ## Opens `path`, binds the handle to `name` for the duration of `body` and
  ## guarantees `closeWait` afterwards (also when `body` raises or is
  ## cancelled). Must be used inside an async proc — the close is awaited.
  ##
  ## ```nim
  ## withAsyncFile(f, "/tmp/data.txt", fmReadWrite):
  ##   await f.write("hello")
  ## ```
  let name = openAsync(path, mode)
  try:
    body
  finally:
    await name.closeWait()

template withAsyncFile*(
    name: untyped, path: string, mode: FileMode, perm: untyped, body: untyped
) =
  ## `withAsyncFile` with an explicit creation permission: `perm` is either a
  ## `set[FilePermission]` or a plain octal int (e.g. `0o600`), resolved
  ## against the matching `openAsync` overload. Same close guarantee as the
  ## perm-less form.
  ##
  ## ```nim
  ## withAsyncFile(f, "/tmp/secret.txt", fmWrite, 0o600):
  ##   await f.write("hello")
  ## ```
  let name = openAsync(path, mode, perm)
  try:
    body
  finally:
    await name.closeWait()
