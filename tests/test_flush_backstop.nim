## White-box backstop test for `flush`'s liveness-poll wait machinery.
##
## `flush` wakes on a completion signal the worker fires, but as a backstop it
## also polls the worker's `exited` flag, so a never-delivered signal cannot
## hang it (a `Future` cannot be failed from a foreign thread, and the loop side
## waits with `noCancel`). This test drives that machinery directly —
## `awaitFlushWorker` / `pollWorkerExit` — against a stand-in worker that settles
## its result and closes the dup fd but NEVER fires the signal, i.e. the
## pathological all-`fireSync`-failed case. With the backstop in place
## `awaitFlushWorker` must still complete, surface the worker's real result and
## leave no fd / thread / event-fd leak behind. Before the backstop existed this
## hung forever and leaked the signal's event fd and the worker thread.
##
## It is a white-box test: it `include`s `posix_flush_close` to reach the private
## `FlushCtx` / `awaitFlushWorker` / `settleFlushSync` symbols. The "never fire
## the signal" behaviour therefore lives here in the test's own worker —
## production `flushWorker` carries no test-only switch. The include is scoped
## to this test module, so it can join the umbrella runner and is imported by
## `test_chronos_file.nim`; `nimble test` compiles it through that runner.

import pkg/chronos/unittest2/asynctests
import helpers

include ../chronos_file/posix_flush_close
{.pop.} # balance the implementation module's `{.push raises: [].}`

proc noFireWorker(ctx: ptr FlushCtx) {.thread.} =
  ## Stand-in for `flushWorker` minus the completion-signal fire: settles the
  ## sync result, closes the dup fd and publishes `exited`, but never calls
  ## `fireSync`. Forces the loop side onto its liveness-poll backstop.
  settleFlushSync(ctx)
  ctx.exited.store(true, moRelease)

proc completesWithin(
    fut: FutureBase, dur: Duration
): Future[bool] {.async: (raises: [CancelledError]).} =
  ## Resolve `true` if `fut` finishes within `dur`, `false` otherwise — *without*
  ## cancelling `fut`. `awaitFlushWorker` defers cancellation, so a genuinely hung
  ## one cannot be awaited or cancelled out of; racing it against a timer lets a
  ## regression surface as a clean failed assertion here instead of wedging the
  ## whole suite.
  let timer = sleepAsync(dur)
  discard await race(fut, FutureBase(timer))
  result = fut.completed()
  if not result:
    echo "completesWithin timed out after ",
      dur, " (finished=", fut.finished, " cancelled=", fut.cancelled, ")"
  await timer.cancelAndWait()

proc newSignal(): ThreadSignalPtr =
  ThreadSignalPtr.new().valueOr:
    raiseAssert "ThreadSignalPtr.new failed: " & error

suite "chronos_file: flush liveness-poll backstop (signal never fired)":
  teardown:
    checkLeaks()

  asyncTest "awaitFlushWorker completes via the backstop when the signal is never fired":
    let path = tempPath(".bin")
    let f = openAsync(path, fmReadWrite)
    await f.write(@[byte 1, 2, 3])
    let signal = newSignal()
    let dupFd = dup(cint(f.fd))
    require dupFd != -1
    var ctx = FlushCtx(signal: signal, fd: dupFd, dataOnly: false)
    var thr: Thread[ptr FlushCtx]
    # The worker syncs and exits without ever firing the signal; the backstop
    # must notice the thread has gone and resolve the wait instead of hanging.
    createThread(thr, noFireWorker, addr ctx)
    let fut = awaitFlushWorker(addr ctx, signal)
    check not fut.finished()
    require await completesWithin(FutureBase(fut), 5.seconds)
    joinThread(thr)
    discard signal.close()
    check ctx.errCode == 0
    f.close()
    removeFile(path)

  asyncTest "the backstop still surfaces the worker's sync result":
    let path = tempPath(".bin")
    let f = openAsync(path, fmReadWrite)
    await f.write(@[byte 9])
    let signal = newSignal()
    let dupFd = dup(cint(f.fd))
    require dupFd != -1
    # A successful fdatasync must still surface as success on the backstop path
    # (the worker settles errCode before exiting, regardless of the signal).
    var ctx = FlushCtx(signal: signal, fd: dupFd, dataOnly: true)
    var thr: Thread[ptr FlushCtx]
    createThread(thr, noFireWorker, addr ctx)
    require await completesWithin(
      FutureBase(awaitFlushWorker(addr ctx, signal)), 5.seconds
    )
    joinThread(thr)
    discard signal.close()
    check ctx.errCode == 0
    f.close()
    removeFile(path)

  asyncTest "cancelling a backstop wait is deferred (noCancel)":
    let path = tempPath(".bin")
    let f = openAsync(path, fmReadWrite)
    await f.write(@[byte 1, 2, 3])
    let signal = newSignal()
    let dupFd = dup(cint(f.fd))
    require dupFd != -1
    var ctx = FlushCtx(signal: signal, fd: dupFd, dataOnly: false)
    var thr: Thread[ptr FlushCtx]
    createThread(thr, noFireWorker, addr ctx)
    let fut = awaitFlushWorker(addr ctx, signal)
    check not fut.finished()
    # Requesting cancellation must not detach the worker; the wait settles as
    # completed (never cancelled) once the worker is observed to have exited.
    fut.cancelSoon()
    require await completesWithin(FutureBase(fut), 5.seconds)
    check fut.completed()
    joinThread(thr)
    discard signal.close()
    f.close()
    removeFile(path)

  asyncTest "two concurrent backstop waits both complete without leaking":
    let path = tempPath(".bin")
    let f = openAsync(path, fmReadWrite)
    await f.write(@[byte 1, 2, 3])
    let
      sa = newSignal()
      sb = newSignal()
      da = dup(cint(f.fd))
      db = dup(cint(f.fd))
    require da != -1 and db != -1
    var
      ca = FlushCtx(signal: sa, fd: da, dataOnly: false)
      cb = FlushCtx(signal: sb, fd: db, dataOnly: true)
    var ta, tb: Thread[ptr FlushCtx]
    createThread(ta, noFireWorker, addr ca)
    createThread(tb, noFireWorker, addr cb)
    let
      fa = awaitFlushWorker(addr ca, sa)
      fb = awaitFlushWorker(addr cb, sb)
    require await completesWithin(FutureBase(allFutures(fa, fb)), 5.seconds)
    check fa.completed() and fb.completed()
    joinThread(ta)
    joinThread(tb)
    discard sa.close()
    discard sb.close()
    f.close()
    removeFile(path)
