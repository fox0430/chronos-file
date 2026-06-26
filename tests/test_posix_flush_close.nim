## Tests for `chronos_file/posix_flush_close`: `flush`, `close`/`closeWait`
## (including their pending-op and draining contracts) and the one-shot helpers
## (`readFileAsync`/`writeFileAsync`/`readFileBytesAsync`/`withAsyncFile`).

from std/posix import mkfifo, Mode

import pkg/chronos/unittest2/asynctests

import ../chronos_file
import helpers

when defined(chronosFileUring):
  import ../chronos_file/uring_io # uringAvailable(): skip io_uring tests on fallback
  import ../chronos_file/posix_handle # readSeekable/writeSeekable: per-chunk seam guard

suite "chronos_file: posix_flush_close (flush, close, one-shot helpers)":
  teardown:
    checkLeaks()

  asyncTest "close fails a pending FIFO read instead of leaking it":
    let path = tempPath(".fifo")
    check mkfifo(cstring(path), Mode(0o600)) == 0
    let r = openAsync(path, fmRead)
    let w = openAsync(path, fmWrite)
    let rfut = r.read(5)
    check not rfut.finished()
    r.close()
    var failed = false
    try:
      discard await rfut
    except AsyncFileError:
      failed = true
    check failed
    w.close()
    removeFile(path)

  asyncTest "close fails a pending FIFO write instead of leaking it":
    # Mirror of the pending-read close test for the write slot: a write that
    # suspended on a full pipe (addWriter2) must be failed by close(), and the
    # writer torn down, so the writeCb cannot later touch the (closed) fd.
    let path = tempPath(".fifo")
    check mkfifo(cstring(path), Mode(0o600)) == 0
    let r = openAsync(path, fmRead)
    let w = openAsync(path, fmWrite)
    # Exceed the pipe buffer so the write suspends on addWriter2.
    let big = newSeq[byte](2 * 1024 * 1024)
    let wfut = w.write(big)
    check not wfut.finished()
    w.close()
    var failed = false
    try:
      await wfut
    except AsyncFileError:
      failed = true
    check failed
    r.close()
    removeFile(path)

  asyncTest "flush succeeds on a regular file":
    let path = tempPath(".bin")
    let f = openAsync(path, fmReadWrite)
    await f.write(@[byte 1, 2, 3])
    await f.flush()
    await f.flush(flushDataOnly)
    f.close()
    removeFile(path)

  asyncTest "flush fails on a FIFO":
    let path = tempPath(".fifo")
    check mkfifo(cstring(path), Mode(0o600)) == 0
    let r = openAsync(path, fmRead)
    let w = openAsync(path, fmWrite)
    var failed = false
    try:
      await w.flush()
    except AsyncFileError:
      failed = true
    check failed
    r.close()
    w.close()
    removeFile(path)

  asyncTest "flush completes asynchronously, not inline":
    let path = tempPath(".bin")
    let f = openAsync(path, fmReadWrite)
    await f.write(@[byte 1, 2, 3])
    let fut = f.flush()
    # Completion is delivered through the dispatcher (ThreadSignal), so the
    # future cannot be finished before poll() runs, even if the worker thread
    # already finished the fsync.
    check not fut.finished()
    await fut
    f.close()
    removeFile(path)

  asyncTest "close during an in-flight flush is safe (dup independence)":
    let path = tempPath(".bin")
    let f = openAsync(path, fmReadWrite)
    await f.write(@[byte 1, 2, 3])
    let fut = f.flush()
    check not fut.finished()
    # The worker syncs its own dup'd descriptor, so closing the handle out from
    # under it must not invalidate the flush; it still completes successfully.
    f.close()
    await fut
    removeFile(path)

  asyncTest "cancelling a flush waits it out instead of detaching (noCancel)":
    let path = tempPath(".bin")
    let f = openAsync(path, fmReadWrite)
    await f.write(@[byte 1, 2, 3])
    let fut = f.flush()
    check not fut.finished()
    # fsync cannot be aborted: cancellation is deferred and the future settles
    # as completed (not cancelled) once the sync finishes.
    await fut.cancelAndWait()
    check fut.completed()
    f.close()
    removeFile(path)

  asyncTest "two concurrent flushes both complete":
    let path = tempPath(".bin")
    let f = openAsync(path, fmReadWrite)
    await f.write(@[byte 1, 2, 3])
    # Each flush owns its dup'd fd, signal and worker thread, so two in flight
    # on the same file must not interfere.
    let fa = f.flush()
    let fb = f.flush(flushDataOnly)
    await allFutures(fa, fb)
    check fa.completed() and fb.completed()
    f.close()
    removeFile(path)

  asyncTest "close is idempotent (#7)":
    let path = tempPath(".bin")
    let f = openAsync(path, fmReadWrite)
    await f.write(@[byte 1])
    f.close()
    f.close()
    removeFile(path)

  asyncTest "operations after close raise AsyncFileError (M2)":
    let path = tempPath(".bin")
    let f = openAsync(path, fmReadWrite)
    await f.write(@[byte 1, 2, 3])
    f.close()

    template rejects(body: untyped) =
      var failed = false
      try:
        body
      except AsyncFileError:
        failed = true
      check failed

    rejects:
      discard await f.read(1)
    rejects:
      discard await f.readAll()
    rejects:
      await f.write(@[byte 9])
    rejects:
      discard f.getFileSize()
    rejects:
      discard f.getFilePos()
    rejects:
      f.setFilePos(0)
    rejects:
      f.setFileSize(0)
    rejects:
      await f.flush()
    rejects:
      discard await f.readAt(0, 1)
    rejects:
      await f.writeAt(0, @[byte 9])
    rejects:
      discard await f.readAllString()
    rejects:
      discard await f.readString(1)
    rejects:
      discard await f.readExactlyString(1)
    # Zero-size positioned ops must be rejected too (they used to silently
    # succeed because the early return skipped the handle check).
    rejects:
      discard await f.readAt(0, 0)
    rejects:
      await f.writeAt(0, "")
    removeFile(path)

  asyncTest "closeWait closes the file and is idempotent (L6)":
    let path = tempPath(".bin")
    let f = openAsync(path, fmReadWrite)
    await f.write(@[byte 1, 2, 3])
    await f.closeWait()
    # Second closeWait and a close() after it are both no-ops (no raise).
    await f.closeWait()
    f.close()
    # The handle is closed, so further I/O is rejected.
    var failed = false
    try:
      discard await f.read(1)
    except AsyncFileError:
      failed = true
    check failed
    removeFile(path)

  asyncTest "closeWait cancels a pending FIFO read gracefully (L6)":
    let path = tempPath(".fifo")
    check mkfifo(cstring(path), Mode(0o600)) == 0
    let r = openAsync(path, fmRead)
    let w = openAsync(path, fmWrite)
    let rfut = r.read(5)
    check not rfut.finished()
    # closeWait should cancel the in-flight read and drain it before closing,
    # so the future ends cancelled rather than failed with EBADF.
    await r.closeWait()
    check rfut.cancelled()
    w.close()
    removeFile(path)

  when defined(chronosFileUring):
    asyncTest "closeWait drains an in-flight io_uring seekable read (B4)":
      # On the io_uring backend a seekable read suspends on the CQE, so it can be
      # in flight at close. closeWait must cancel and drain it (tracked via
      # `seekFuts`) before closing the fd, so the read's awaiter sees a graceful
      # CancelledError and the kernel is done with the buffer first — the seekable
      # analog of the FIFO closeWait above.
      if not uringAvailable():
        skip() # backend compiled in but unusable here: no seekable op stays in flight
        return
      let path = tempPath(".bin")
      block:
        let g = openAsync(path, fmReadWrite)
        await g.write(newSeq[byte](256 * 1024))
        g.close()
      let f = openAsync(path, fmReadWrite)
      let rfut = f.read(256 * 1024) # suspends on the io_uring CQE
      check not rfut.finished()
      check f.seekFuts.len == 1 # tracked as the in-flight seekable op
      await f.closeWait()
      check f.isClosed
      expect(CancelledError):
        discard await rfut # the drained read surfaces as cancelled
      removeFile(path)

    asyncTest "closeWait drains every concurrent in-flight io_uring positioned op":
      # Positioned `*At` ops take no offset slot, so several can be in flight at
      # once. Each must be tracked (in `seekFuts`) and drained by closeWait — not
      # just the most recently submitted: an undrained op's CQE would otherwise land
      # in a caller buffer freed after the fd is closed (use-after-free). This is
      # the regression guard for that single-slot gap.
      if not uringAvailable():
        skip() # backend compiled in but unusable here: ops complete synchronously
        return
      let path = tempPath(".bin")
      block:
        let g = openAsync(path, fmReadWrite)
        await g.write(newSeq[byte](512 * 1024))
        g.close()
      let f = openAsync(path, fmReadWrite)
      var b1 = newSeq[byte](256 * 1024)
      var b2 = newSeq[byte](256 * 1024)
      # Two positioned reads, launched without awaiting in between, so both suspend
      # on the ring concurrently.
      let f1 = f.readBufferAt(0, addr b1[0], b1.len)
      let f2 = f.readBufferAt(256 * 1024, addr b2[0], b2.len)
      check not f1.finished()
      check not f2.finished()
      check f.seekFuts.len == 2 # both tracked, neither overwritten by the other
      await f.closeWait()
      check f.isClosed
      # Both ops were drained (settled) before the fd closed — neither was left in
      # flight against a closed descriptor. The drain surfaces as cancellation; a
      # sibling whose CQE lands during the first drain may instead complete normally
      # (also safe — the kernel is then done with its buffer). Either way it must not
      # have failed.
      check f1.finished() and not f1.failed()
      check f2.finished() and not f2.failed()
      removeFile(path)

    asyncTest "synchronous close with an in-flight io_uring op raises (use closeWait)":
      # Synchronous `close` cannot await a drain, so rather than close the fd out
      # from under a suspended io_uring op — risking a deferred-submit fd reuse or a
      # write into a since-freed buffer — it refuses loudly and points the caller at
      # `closeWait`, which cancels and drains first. (closeWait still reaches the
      # shared close body, but only once every tracked op is finished.)
      if not uringAvailable():
        skip() # backend compiled in but unusable here: no seekable op stays in flight
        return
      let path = tempPath(".bin")
      block:
        let g = openAsync(path, fmReadWrite)
        await g.write(newSeq[byte](256 * 1024))
        g.close()
      let f = openAsync(path, fmReadWrite)
      let rfut = f.read(256 * 1024) # suspends on the io_uring CQE
      check not rfut.finished()
      check f.seekFuts.len == 1
      expect(AsyncFileError):
        f.close() # refused while the op is in flight
      check not f.isClosed # nothing was closed out from under the op
      # closeWait drains the op and closes gracefully; the read surfaces as cancelled.
      await f.closeWait()
      check f.isClosed
      expect(CancelledError):
        discard await rfut
      removeFile(path)

    asyncTest "a seam chunk re-entered once closeWait is draining is cancelled, not tracked":
      # Regression guard for the closeWait drain race: closeWait snapshots
      # `seekFuts`, then drains each entry. A multi-chunk write loop
      # (`writeBufferSeekable`/`writeBufferAt`) or `readLine`'s refill loop that
      # resumes *during* that drain — because an earlier sibling op completed
      # naturally — would otherwise submit a fresh seam op and `trackSeekFut` it
      # *outside* the snapshot. `closeImpl` would then see that untracked leaf still
      # in flight and raise, which `closeWait` swallows: the fd is never closed (a
      # leak), and the deferred submit could reach an already-closed/reused fd.
      #
      # The public procs all guard the *first* chunk at entry, but a resumed loop
      # re-enters `readSeekable`/`writeSeekable` directly, so the per-chunk guard
      # lives in the seam. Two things must hold:
      #   1. the resubmit is refused (never tracked), so no leaf escapes the
      #      snapshot — the fd-leak fix; and
      #   2. it is refused as a *cancellation*, not an `AsyncFileError`: the chunk
      #      belongs to one caller op already in flight when closeWait snapshotted,
      #      so its awaiter is owed the `CancelledError` closeWait documents (like
      #      the siblings the drain cancels directly). A brand-new op started after
      #      closeWait is the different case the entry guards still reject with
      #      `AsyncFileError`.
      # Drive the seam directly with `f.closing` set (the exact state closeWait
      # establishes before it suspends) to exercise both.
      if not uringAvailable():
        skip() # backend compiled in but unusable here: the seam never tracks an op
        return
      let path = tempPath(".bin")
      block:
        let g = openAsync(path, fmReadWrite)
        await g.write(newSeq[byte](64 * 1024))
        g.close()
      let f = openAsync(path, fmReadWrite)
      var buf = newSeq[byte](4096)

      # Simulate the drain window: `closing` set, `seekFuts` already snapshotted.
      f.closing = true
      let wfut = writeSeekable(f, addr buf[0], buf.len, 0, "write")
      expect(CancelledError):
        discard await wfut # refused as a cancellation, not submitted
      let rfut = readSeekable(f, addr buf[0], buf.len, 0, "read")
      expect(CancelledError):
        discard await rfut # refused as a cancellation, not submitted
      check f.seekFuts.len == 0 # neither was tracked → closeImpl closes cleanly

      # A brand-new public op in the same window still reports AsyncFileError, per
      # closeWait's "new operations are rejected with AsyncFileError" clause — the
      # entry guards, not the seam's `f.closing` branch.
      expect(AsyncFileError):
        discard await f.readAt(0, 4)
      expect(AsyncFileError):
        await f.writeAt(0, @[byte 1])

      # Clear the simulated flag and close for real (no op is in flight, so the
      # drain is empty and the fd is actually released — no leak).
      f.closing = false
      await f.closeWait()
      check f.isClosed
      removeFile(path)

  asyncTest "close during a closeWait drain is a no-op (graceful contract)":
    let path = tempPath(".fifo")
    check mkfifo(cstring(path), Mode(0o600)) == 0
    let r = openAsync(path, fmRead)
    let w = openAsync(path, fmWrite)
    let rfut = r.read(5)
    check not rfut.finished()
    let cw = r.closeWait()
    # closeWait has set `closing`; a synchronous close slipped into the drain
    # window must be a no-op instead of failing the pending read with EBADF,
    # so the awaiter still sees the graceful CancelledError.
    r.close()
    await cw
    check rfut.cancelled()
    w.close()
    removeFile(path)

  asyncTest "operations issued while closeWait is draining are rejected":
    # closeWait suspends in cancelAndWait before the descriptor is closed; an
    # operation slipped into that window must be rejected up front (closing
    # state) rather than racing the close and failing with EBADF.
    let path = tempPath(".fifo")
    check mkfifo(cstring(path), Mode(0o600)) == 0
    let r = openAsync(path, fmRead)
    let w = openAsync(path, fmWrite)
    let rfut = r.read(5)
    check not rfut.finished()
    # closeWait runs synchronously up to its first await (the cancelAndWait on
    # the pending read), so by the time it returns a future the closing flag is
    # already set and new I/O must be rejected.
    let cfut = r.closeWait()
    var failed = false
    try:
      discard await r.read(1)
    except AsyncFileError:
      failed = true
    check failed
    await cfut
    check rfut.cancelled()
    w.close()
    removeFile(path)

  asyncTest "writeFileAsync / readFileAsync / readFileBytesAsync roundtrip":
    let path = tempPath(".txt")
    await writeFileAsync(path, "hello async")
    check (await readFileAsync(path)) == "hello async"
    check (await readFileBytesAsync(path)) == toBytes("hello async")
    # The seq[byte] overload truncates and rewrites.
    await writeFileAsync(path, @[byte 1, 2, 3])
    check (await readFileBytesAsync(path)) == @[byte 1, 2, 3]
    removeFile(path)

  asyncTest "readFileAsync on a missing file raises AsyncFileOsError":
    let path = tempPath(".missing")
    removeFile(path)
    var raised = false
    try:
      discard await readFileAsync(path)
    except AsyncFileOsError:
      raised = true
    check raised

  asyncTest "writeFileAsync honours the perm argument":
    let path = tempPath(".bin")
    await writeFileAsync(path, "x", {fpUserRead, fpUserWrite})
    check getFilePermissions(path) == {fpUserRead, fpUserWrite}
    removeFile(path)

  asyncTest "withAsyncFile closes the handle after the body":
    let path = tempPath(".txt")
    withAsyncFile(f, path, fmReadWrite):
      await f.write("hi")
      f.setFilePos(0)
      check (await f.readAllString()) == "hi"
    # The template's finally must have closed the handle.
    var closed = false
    try:
      discard f.getFilePos()
    except AsyncFileError:
      closed = true
    check closed
    removeFile(path)

  asyncTest "withAsyncFile accepts a perm argument (set and octal int)":
    let p1 = tempPath(".bin")
    withAsyncFile(f, p1, fmWrite, {fpUserRead, fpUserWrite}):
      await f.write("x")
    check getFilePermissions(p1) == {fpUserRead, fpUserWrite}
    removeFile(p1)

    let p2 = tempPath(".bin")
    withAsyncFile(g, p2, fmWrite, 0o600):
      await g.write("y")
    check getFilePermissions(p2) == {fpUserRead, fpUserWrite}
    # The close guarantee carries over from the perm-less form.
    check g.isClosed()
    removeFile(p2)

  asyncTest "withAsyncFile closes the handle when the body raises":
    let path = tempPath(".txt")
    block:
      let f = openAsync(path, fmWrite)
      await f.write(@[byte 1])
      f.close()
    var captured: AsyncFile
    var raised = false
    try:
      withAsyncFile(g, path, fmRead):
        captured = g
        await g.write(@[byte 2]) # fails: read-only handle
    except AsyncFileError:
      raised = true
    check raised
    # The finally ran despite the raise, so the handle is closed.
    var closed = false
    try:
      discard captured.getFilePos()
    except AsyncFileError:
      closed = true
    check closed
    removeFile(path)
