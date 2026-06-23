## Durability (`flush` via a worker thread), teardown (`close`/`closeWait`) and
## the one-shot file helpers built on the rest of the API.

import std/[atomics, posix, typedthreads]
from std/os import FilePermission

import pkg/chronos
import pkg/chronos/[osutils, oserrno]

when not defined(nimdoc):
  import pkg/chronos/threadsync
else:
  # Under `nim doc`, chronos 4.2.2's doc-stub branch omits
  # `contains(PDispatcher, AsyncFD)`, which `threadsync` needs, so importing it
  # fails to compile. Stub the few symbols we use for doc builds only.
  # See also https://github.com/status-im/nim-chronos/pull/657
  type
    ThreadSignal = object
    ThreadSignalPtr = ptr ThreadSignal

  proc new(t: typedesc[ThreadSignalPtr]): Result[ThreadSignalPtr, string] =
    discard

  proc close(signal: ThreadSignalPtr): Result[void, string] =
    discard

  proc fireSync(
      signal: ThreadSignalPtr, timeout = InfiniteDuration
  ): Result[bool, string] =
    discard

  proc wait(
      signal: ThreadSignalPtr
  ): Future[void] {.async: (raises: [AsyncError, CancelledError]).} =
    discard

import common, posix_handle, posix_io

{.push raises: [].}

type FlushCtx = object
  ## State shared between `flush` and its worker thread. Holds no GC-managed
  ## fields, so a `ptr` to it may cross threads. Owned by `flush`: it lives in
  ## the async proc's environment, which `noCancel` keeps alive until `flush`
  ## returns (after it has `joinThread`-ed the worker). The worker may therefore
  ## keep writing `ctx` for its whole lifetime — up to and including the final
  ## `exited` store — even after firing the completion signal.
  signal: ThreadSignalPtr
  fd: cint ## dup of the file's descriptor; closed by the worker when done.
  dataOnly: bool
  errCode: int32 ## errno of a failed sync, or 0 on success.
  exited: Atomic[bool]
    ## Set to `true` with `moRelease` by the worker as its very last action,
    ## immediately before it returns. The watchdog loads it with `moAcquire`, so
    ## observing `true` synchronises-with the final `errCode` and closed-fd
    ## state. Because the worker only ever sets it last, `exited == true` already
    ## implies the thread has fully run — an unscheduled or still-running worker
    ## leaves it at its `false` zero value — so no separate "has it started yet?"
    ## flag is needed. Replaces `running(thr[])` to avoid relying on the
    ## implementation-defined memory semantics of `std/typedthreads`.

const workerPollInterval = 10.milliseconds
  ## Backstop cadence for noticing that the flush worker has exited when its
  ## completion signal never arrives. Only the pathological all-`fireSync`-failed
  ## path ever waits on this; the healthy path wakes on the signal instead.

proc settleFlushSync(ctx: ptr FlushCtx) =
  ## Runs the blocking fsync/fdatasync, records the resulting errno in
  ## `ctx.errCode` (0 on success) and closes the dup'd fd. Factored out of
  ## `flushWorker` so the backstop test can reuse the exact same settle logic in
  ## a stand-in worker that deliberately never fires the completion signal.
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

proc flushWorker(ctx: ptr FlushCtx) {.thread.} =
  ## Runs the blocking fsync/fdatasync off the event loop. Settles the ctx
  ## (errCode), returns the dup'd fd, fires the completion signal and finally
  ## publishes `exited` — in that order, so when the loop side wakes up the
  ## result is final and no fd is left behind. `ctx` stays valid for the whole
  ## of this proc: `flush` keeps it alive (via `noCancel`) until after it has
  ## `joinThread`-ed this worker, so writing `ctx.exited` after `fireSync` is
  ## safe even though the loop side may already have woken on the signal.
  settleFlushSync(ctx)

  for _ in 0 ..< 3:
    # Fire the completion signal so the loop side wakes promptly. A Future cannot
    # be failed from a foreign thread, so this is the only event-driven wakeup;
    # `fireSync` already blocks internally on a full signal pipe, so an error here
    # is exceptional (EBADF-class). Retry a few times as a best-effort mitigation.
    # A signal lost despite all retries no longer hangs `flush`: the loop side
    # also polls this thread's liveness as a backstop (see `awaitFlushWorker`),
    # and the worker has already settled `ctx.errCode` above. Firing still matters
    # because it keeps the common path event-driven rather than waiting out a poll
    # interval. The `break` stops after the first success so we never fire twice;
    # the only `ctx` access after a successful fire is the `exited` store below,
    # which is safe (see this proc's docstring).
    if ctx.signal.fireSync().isOk():
      break
  # Publish thread exit. `ctx` remains valid until `flush` returns (it is a
  # local variable kept alive by `noCancel`), so this store is safe even after
  # a successful `fireSync`.
  ctx.exited.store(true, moRelease)

proc pollWorkerExit(
    ctx: ptr FlushCtx
): Future[void] {.async: (raises: [CancelledError]).} =
  ## Completes once the worker thread has returned. The worker sets `ctx.exited`
  ## with `moRelease` as its very last action, so loading it with `moAcquire`
  ## synchronises-with the final `errCode` and closed-fd state — independently
  ## of whether it managed to fire the completion signal. An unscheduled or
  ## still-running worker leaves `exited` at its `false` zero value, so polling
  ## it alone never mistakes "not finished yet" for "already exited".
  while not ctx.exited.load(moAcquire):
    await sleepAsync(workerPollInterval)

proc awaitFlushWorker(
    ctx: ptr FlushCtx, signal: ThreadSignalPtr
): Future[void] {.async: (raises: []).} =
  ## Suspend until the flush worker has finished, leaving it ready to be joined.
  ## The worker fires `signal` after settling the result and closing the dup fd,
  ## so the healthy wakeup is event-driven and prompt. As a backstop for the
  ## pathological case where every `fireSync` fails — the signal is never
  ## delivered and `signal.wait()` would otherwise block forever — race the wait
  ## against a watchdog that polls the worker's liveness directly.
  ##
  ## Never raises and never fails the flush on a wakeup error: the worker always
  ## settles its result in `ctx`, so a signal/registration hiccup is irrelevant
  ## once the thread has stopped running. Cancellation is deferred (the fsync
  ## cannot be aborted), which is why every await here is either wrapped in
  ## `noCancel` or operates on an `OwnCancelSchedule` future (the final
  ## `cancelAndWait` calls).
  let
    signalWait = signal.wait()
    watchdog = pollWorkerExit(ctx)
  discard await noCancel(race(FutureBase(signalWait), FutureBase(watchdog)))
  if not signalWait.completed():
    # The wakeup came from the watchdog (worker already exited) or from a
    # *failed* `signal.wait()` (e.g. an event-fd registration/read error, which
    # can fire while the fsync is still running). Wait for the watchdog to
    # observe the worker exit so the caller's `joinThread` only reaps it and
    # never blocks the event loop on a still-running fsync.
    await noCancel(watchdog)
  # Tear down both arms; whichever already finished is a no-op. Cancelling the
  # signal wait removes its event-fd reader before the caller closes the signal.
  await signalWait.cancelAndWait()
  await watchdog.cancelAndWait()

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
  ## **Signal backstop:** the worker tries to fire a completion signal so the
  ## event loop wakes promptly. If signal registration, delivery or waiting fails
  ## for any reason, `flush` does not raise an error for that — the worker's
  ## result is still collected once the thread exits, and any actual sync failure
  ## is reported as an `AsyncFileError` from `ctx.errCode`.
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

  # `ctx` and `thr` live in this proc's environment. `awaitFlushWorker` defers
  # cancellation internally with `noCancel`, so this proc runs to completion and
  # both outlive the worker.
  var ctx = FlushCtx(signal: signal, fd: dupFd, dataOnly: kind == flushDataOnly)
  var thr: Thread[ptr FlushCtx]
  try:
    createThread(thr, flushWorker, addr ctx)
  except CatchableError as e:
    discard closeFd(dupFd)
    discard signal.close()
    raise newAsyncFileError("flush: cannot create worker thread: " & e.msg)
  # Wait for the worker to finish: event-driven on the completion signal, with a
  # liveness-poll backstop so a never-delivered signal can't hang us. This never
  # raises and defers cancellation, so the worker is always reaped and the signal
  # always closed below — no fd/thread leak even if every `fireSync` failed.
  await awaitFlushWorker(addr ctx, signal)
  # The worker has either fired the signal (after the fsync is complete) or been
  # observed exited by the watchdog. `joinThread` here reaps the thread; it never
  # blocks the loop on a still-running fsync.
  joinThread(thr)
  # Closing the signal unregisters its event fd from this thread's dispatcher, so
  # it must happen here on the loop thread, not in the worker.
  discard signal.close()

  if ctx.errCode != 0:
    raise newAsyncFileOsError(OSErrorCode(ctx.errCode), "flush")

proc closeImpl(f: AsyncFile) {.raises: [AsyncFileError].} =
  ## Shared body of `close` and `closeWait`: fails any in-flight pipe/FIFO
  ## futures, unregisters non-seekable fds and closes the descriptor. Carries
  ## no `closing` guard, so `closeWait` can call it while `f.closing` is set
  ## (the public `close` adds that guard).
  ##
  ## Only the non-seekable EAGAIN futures (`readFut`/`writeFut`) are drained here.
  ## A seekable op completes synchronously today, so `seekOpInFlight` is never set
  ## at close and there is nothing seekable to drain. Once the seam suspends
  ## (io_uring) a suspended seekable op will need a cancellable future tracked and
  ## drained here — see `IO_URING_TODO.md` (A4/B4).
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
