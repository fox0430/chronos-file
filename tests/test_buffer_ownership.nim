## Tests for the low-level buffer-ownership / cancellation contract
##
## The low-level pointer procs (`readBuffer`/`writeBuffer`/`readBufferAt`/
## `writeBufferAt`) take a caller-owned `buf`. Their contract: keep `buf` valid
## until the returned future *settles*, because cancellation drains the in-flight
## kernel op before the future resolves (zero-copy is preserved; the high-level
## `read`/`write`/`readAt`/`writeAt` family is the owning, fully cancel-safe
## surface instead).
##
## White-box: on today's synchronous backend the seekable seam (`pread`/`pwrite`)
## completes inline, so a seekable `*Buffer*` future is already finished before it
## is awaited — there is no suspension point at which a cancel could tear the op
## while the kernel still holds `buf`. That synchronous-completion invariant is
## exactly what makes the current zero-copy contract trivially cancel-safe. When
## the io_uring backend lands the seam will suspend (the future is no longer
## finished on return) and the drain-on-cancel wrapper takes over; these checks
## flip at that boundary and force the buffer-ownership contract to be revisited.

import pkg/chronos/unittest2/asynctests

import ../chronos_file
import helpers

when defined(chronosFileUring):
  # `uringAvailable()` lets the io_uring-only tests skip gracefully when the
  # backend is compiled in but unusable at runtime (old kernel / sandbox), where
  # the seam falls back to synchronous pread and would not suspend.
  import ../chronos_file/uring_io

suite "chronos_file: low-level buffer ownership / cancellation contract":
  teardown:
    checkLeaks()

  when defined(chronosFileUring):
    asyncTest "seekable low-level buffer ops suspend; cancel drains before settling":
      # io_uring backend (A3): the seam now suspends on the CQE, so the
      # synchronous-completion invariant the sync backend relies on is gone — the
      # zero-copy low-level ops are no longer finished on return. Cancellation is
      # therefore reachable, and must DRAIN the in-flight kernel op before the
      # future settles, keeping the caller's `buf` valid for the whole op. This is
      # the drain-on-cancel contract the sync-backend test below can only assert
      # the *absence* of a cancel point for.
      if not uringAvailable():
        skip() # backend compiled in but unusable here: seam stays synchronous
        return
      let path = tempPath(".bin")
      let f = openAsync(path, fmReadWrite)

      var src = @[byte 10, 20, 30, 40]
      block:
        let fut = f.writeBuffer(addr src[0], src.len)
        check not fut.finished() # suspends on the CQE now
        await fut

      f.setFilePos(0)
      var dst = newSeq[byte](src.len)
      block:
        let fut = f.readBuffer(addr dst[0], dst.len)
        check not fut.finished()
        check (await fut) == src.len
      check dst == src

      # Positioned variants funnel through the same seam and likewise suspend.
      var src2 = @[byte 1, 2]
      block:
        let fut = f.writeBufferAt(0, addr src2[0], src2.len)
        check not fut.finished()
        await fut
      var dst2 = newSeq[byte](2)
      block:
        let fut = f.readBufferAt(0, addr dst2[0], dst2.len)
        check not fut.finished()
        check (await fut) == 2
      check dst2 == src2

      # Drain-on-cancel: a positioned read into the caller's `dst3` is cancelled
      # in flight. `cancelAndWait` must not return until the op has drained (so the
      # kernel is done with `dst3`), leaving the future Cancelled. A use-after-free
      # here would surface as a crash/leak under the teardown's `checkLeaks`.
      var dst3 = newSeq[byte](2)
      block:
        let fut = f.readBufferAt(0, addr dst3[0], dst3.len)
        check not fut.finished()
        await fut.cancelAndWait()
        check fut.cancelled()

      f.close()
      removeFile(path)
  else:
    asyncTest "seekable low-level buffer ops complete synchronously (no cancel point today)":
      let path = tempPath(".bin")
      let f = openAsync(path, fmReadWrite)

      # writeBuffer: the seam runs inline, so the returned future is already
      # finished before we await it. No suspension == no cancellation point at
      # which the kernel could still be touching `src` after a cancel. (Once
      # io_uring suspends the seam this check fails — see the module doc / A3.)
      var src = @[byte 10, 20, 30, 40]
      block:
        let fut = f.writeBuffer(addr src[0], src.len)
        check fut.finished()
        await fut

      f.setFilePos(0)
      var dst = newSeq[byte](src.len)
      block:
        let fut = f.readBuffer(addr dst[0], dst.len)
        check fut.finished()
        check (await fut) == src.len
      check dst == src

      # The positioned *At variants funnel through the same seekable seam, so they
      # share the synchronous-completion invariant.
      var src2 = @[byte 1, 2]
      block:
        let fut = f.writeBufferAt(0, addr src2[0], src2.len)
        check fut.finished()
        await fut
      var dst2 = newSeq[byte](2)
      block:
        let fut = f.readBufferAt(0, addr dst2[0], dst2.len)
        check fut.finished()
        check (await fut) == 2
      check dst2 == src2

      f.close()
      removeFile(path)

  asyncTest "high-level read owns its buffer (independent of the file)":
    # The high-level read family allocates impl-owned storage, so the returned
    # seq is the caller's to keep and mutate and no caller pointer is ever handed
    # to the kernel — that is what makes it the fully cancel-safe surface. (The
    # real non-seekable cancel-safety is covered by the FIFO cancel tests in
    # test_posix_io.nim; here we pin the ownership/independence on a seekable
    # file, where the contract is observable without a suspension point.)
    let path = tempPath(".bin")
    let f = openAsync(path, fmReadWrite)
    await f.write(@[byte 1, 2, 3, 4])

    f.setFilePos(0)
    var got = await f.read(4)
    check got == @[byte 1, 2, 3, 4]

    # Mutating the returned buffer must not reach back into the file: a fresh
    # read still sees the original bytes, proving the storage is owned, not a
    # view aliased onto anything the op kept.
    got[0] = 99
    check (await f.readAt(0, 4)) == @[byte 1, 2, 3, 4]

    f.close()
    removeFile(path)
