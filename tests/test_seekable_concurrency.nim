## Tests for the seekable single-in-flight offset contract (IO_URING_TODO.md A2).
##
## Implicit-offset ops (`read`/`write`/`readLine` and low-level
## `readBuffer`/`writeBuffer`) share one in-flight slot per seekable handle: a
## second concurrent one raises `AsyncFileBusyError`. Positioned reads
## (`readAt`/`readBufferAt`) ignore the slot; positioned writes and positioning
## ops are turned away while it is held. No-op for non-seekable fds (they use the
## `readFut`/`writeFut` guards). See `AsyncFileBusyError`.
##
## White-box: the seam (`pread`/`pwrite`) completes synchronously today, so two
## ops never actually overlap yet — the busy path is unreachable black-box. These
## tests drive the guard directly: setting `seekOpInFlight` stands in for "an op
## is suspended mid-flight", then real ops are shown to reject and to recover once
## it clears. Live overlap becomes reachable once the seam suspends (io_uring).

from std/posix import mkfifo, Mode

import pkg/chronos/unittest2/asynctests

import ../chronos_file
import ../chronos_file/posix_handle # acquireOffsetGuard / releaseOffsetGuard
import helpers

when defined(chronosFileUring):
  import ../chronos_file/uring_io # uringAvailable(): skip io_uring tests on fallback

suite "chronos_file: seekable single-in-flight offset contract":
  teardown:
    checkLeaks()

  # Rejections are asserted with unittest2's `expect(AsyncFileBusyError): ...`
  # (re-exported via asynctests) rather than a hand-rolled try/except — it also
  # fails clearly if the wrong exception, or none, is raised.

  asyncTest "implicit-offset ops are rejected while the seekable slot is held":
    let path = tempPath(".bin")
    let f = openAsync(path, fmReadWrite)
    await f.write(@[byte 0, 1, 2, 3, 4, 5, 6, 7])
    f.setFilePos(0)

    # Stand in for an implicit-offset op suspended mid-flight (the seam does not
    # yield yet, so hold the slot directly).
    f.seekOpInFlight = true

    # Every implicit-offset entry point shares the slot, so each is rejected.
    # That the rejection leaves the slot held (so the next call also rejects) is
    # implicit in this whole chain succeeding, and asserted explicitly below.
    expect(AsyncFileBusyError):
      discard await f.read(4)
    expect(AsyncFileBusyError):
      discard await f.readString(4)
    expect(AsyncFileBusyError):
      discard await f.readAll()
    expect(AsyncFileBusyError):
      discard await f.readLine()
    expect(AsyncFileBusyError):
      await f.write(@[byte 9])
    var dst = newSeq[byte](4)
    expect(AsyncFileBusyError):
      discard await f.readBuffer(addr dst[0], dst.len)
    var src = @[byte 9]
    expect(AsyncFileBusyError):
      await f.writeBuffer(addr src[0], src.len)

    # The final rejection also left the slot held (otherwise the chain above
    # would have stopped rejecting); assert it explicitly. A rejected op must
    # not have touched the file position or contents either.
    check f.seekOpInFlight
    check f.getFilePos() == 0

    # Releasing the slot restores normal use.
    f.seekOpInFlight = false
    check (await f.read(8)) == @[byte 0, 1, 2, 3, 4, 5, 6, 7]

    f.close()
    removeFile(path)

  asyncTest "positioned read *At family ignores the offset slot":
    let path = tempPath(".bin")
    let f = openAsync(path, fmReadWrite)
    await f.write(@[byte 0, 1, 2, 3, 4, 5, 6, 7])

    # Hold the implicit-offset slot: positioned reads are offset-independent and
    # touch no shared state, so they must still proceed (if they took the guard
    # they would raise here).
    f.seekOpInFlight = true
    check (await f.readAt(4, 4)) == @[byte 4, 5, 6, 7]
    var dst = newSeq[byte](2)
    check (await f.readBufferAt(2, addr dst[0], 2)) == 2
    check dst == @[byte 2, 3]

    # The positioned reads never touch the slot, so it is still held exactly as
    # we left it.
    check f.seekOpInFlight
    f.seekOpInFlight = false

    f.close()
    removeFile(path)

  asyncTest "positioned writes and positioning ops reject while the slot is held":
    let path = tempPath(".bin")
    let f = openAsync(path, fmReadWrite)
    await f.write(@[byte 0, 1, 2, 3, 4, 5, 6, 7])

    # Hold the slot: writeAt/writeBufferAt and setFilePos/setFileSize must drop
    # the read-ahead (touching the shared offset), so each is turned away while a
    # seekable implicit-offset op is in flight.
    f.seekOpInFlight = true
    expect(AsyncFileBusyError):
      await f.writeAt(0, @[byte 100])
    var src = @[byte 50, 51]
    expect(AsyncFileBusyError):
      await f.writeBufferAt(6, addr src[0], 2)
    expect(AsyncFileBusyError):
      f.setFilePos(0)
    expect(AsyncFileBusyError):
      f.setFileSize(4)

    # The rejected ops touched nothing: contents and the slot are unchanged.
    # (A positioned read is allowed and confirms the bytes were not overwritten.)
    check (await f.readAt(0, 8)) == @[byte 0, 1, 2, 3, 4, 5, 6, 7]
    check f.seekOpInFlight

    # Releasing the slot restores the positioned write / positioning ops.
    f.seekOpInFlight = false
    await f.writeAt(0, @[byte 100])
    check (await f.readAt(0, 1)) == @[byte 100]

    f.close()
    removeFile(path)

  asyncTest "setFilePos validates its argument before the busy check":
    # setFilePos runs the `pos < 0` validation *before* checkOffsetIdle, so an
    # invalid position is always reported as the deterministic argument error
    # rather than being masked by the transient AsyncFileBusyError when the slot
    # happens to be held.
    let path = tempPath(".bin")
    let f = openAsync(path, fmReadWrite)
    await f.write(@[byte 0, 1, 2, 3])

    f.seekOpInFlight = true
    var msg = ""
    try:
      f.setFilePos(-1)
    except AsyncFileBusyError:
      msg = "BUSY" # ordering regressed: busy check ran before validation
    except AsyncFileError as e:
      msg = e.msg
    check msg.startsWith("negative file position")

    # A non-negative position while the slot is held still rejects as busy.
    expect(AsyncFileBusyError):
      f.setFilePos(0)
    check f.seekOpInFlight

    f.seekOpInFlight = false
    f.close()
    removeFile(path)

  asyncTest "a completed implicit-offset op releases the slot":
    let path = tempPath(".bin")
    let f = openAsync(path, fmReadWrite)

    # Each op acquires and releases the slot within its (currently synchronous)
    # run, so the flag is back to false afterwards and a later op is not falsely
    # rejected.
    await f.write(@[byte 1, 2, 3])
    check f.seekOpInFlight == false
    f.setFilePos(0)
    discard await f.read(2)
    check f.seekOpInFlight == false
    f.setFilePos(0)
    discard await f.readLine()
    check f.seekOpInFlight == false

    f.setFilePos(0)
    check (await f.read(3)) == @[byte 1, 2, 3]

    f.close()
    removeFile(path)

  asyncTest "multi-chunk reads hold the slot across every chunk":
    # readAll chunks at 64 KiB and readLine refills at 4 KiB, so a payload larger
    # than those forces several seam calls within one logical op. Each op takes
    # the slot once and holds it across all chunks (every chunk passes
    # alreadyGuarded = true), so it must neither falsely reject itself by
    # re-acquiring a held slot (double-acquire) nor leak the slot afterwards.
    let path = tempPath(".bin")
    let f = openAsync(path, fmReadWrite)

    var payload = newSeq[byte](200 * 1024)
    for i in 0 ..< payload.len:
      payload[i] = byte((i mod 9) + 1) # 1..9: never \n (10) or \c (13)
    await f.write(payload)
    check f.seekOpInFlight == false

    # readAll spans multiple 64 KiB chunks and releases the slot.
    f.setFilePos(0)
    check (await f.readAll()) == payload
    check f.seekOpInFlight == false

    # readExactly of the whole payload.
    f.setFilePos(0)
    check (await f.readExactly(payload.len)) == payload
    check f.seekOpInFlight == false

    # readLine reads one terminator-free line spanning many 4 KiB refills.
    f.setFilePos(0)
    let line = await f.readLine()
    check line.isSome
    check line.get().len == payload.len
    check f.seekOpInFlight == false

    f.close()
    removeFile(path)

  test "acquireOffsetGuard rejects re-entry until released (seekable)":
    let path = tempPath(".bin")
    let f = openAsync(path, fmReadWrite)

    acquireOffsetGuard(f)
    check f.seekOpInFlight
    expect(AsyncFileBusyError):
      acquireOffsetGuard(f)
    check f.seekOpInFlight # a rejected acquire leaves the slot held

    releaseOffsetGuard(f)
    check not f.seekOpInFlight
    acquireOffsetGuard(f) # free again
    check f.seekOpInFlight
    releaseOffsetGuard(f)

    f.close()
    removeFile(path)

  when defined(chronosFileUring):
    asyncTest "a real in-flight seekable op rejects a concurrent one (io_uring)":
      # With the io_uring seam suspending, two implicit-offset ops genuinely
      # overlap — no white-box `seekOpInFlight` poking needed. The first holds the
      # single offset slot across its CQE wait; a second implicit-offset op is
      # rejected with AsyncFileBusyError, while an offset-independent positioned
      # read proceeds.
      if not uringAvailable():
        skip() # backend compiled in but unusable here: ops complete synchronously
        return
      let path = tempPath(".bin")
      let f = openAsync(path, fmReadWrite)
      await f.write(@[byte 0, 1, 2, 3, 4, 5, 6, 7])
      f.setFilePos(0)

      let r1 = f.read(4) # runs synchronously up to the seam await: slot now held
      check not r1.finished()
      check f.seekOpInFlight
      expect(AsyncFileBusyError):
        discard await f.read(4) # concurrent implicit-offset op -> busy

      # A positioned read touches no shared state and runs concurrently on the ring.
      check (await f.readAt(4, 4)) == @[byte 4, 5, 6, 7]

      # The first op still completes correctly and frees the slot.
      check (await r1) == @[byte 0, 1, 2, 3]
      check not f.seekOpInFlight

      f.close()
      removeFile(path)

    asyncTest "append-mode writes stay off the io_uring drain set (carve-out)":
      # Append has no io_uring write-at-end mode, so `writeSeekable` always takes
      # the synchronous `write` tail for an `fmAppend` fd: the write completes
      # inline (never suspends on the ring) and is never registered with
      # `trackSeekFut`, so `closeWait`/`closeImpl` have nothing to drain for it.
      # This locks that carve-out even with a usable ring present — if a later
      # change routed append through io_uring, the write would suspend
      # (`not finished()`) and this test would fail, forcing the author to confirm
      # `trackSeekFut` is wired (else `closeWait` would miss the in-flight op). The
      # companion `assert not f.appendMode` in `writeSeekable` guards the same
      # invariant on the code side.
      if not uringAvailable():
        skip() # backend compiled in but unusable: every write is synchronous anyway
        return
      let path = tempPath(".log")
      let f = openAsync(path, fmAppend)

      let w = f.write(@[byte 1, 2, 3, 4])
      check w.finished() # append is synchronous: no ring suspend...
      check f.seekFuts.len == 0 # ...so nothing is tracked for closeWait to drain
      await w

      # A second append behaves identically; the drain set stays empty throughout.
      let w2 = f.write(@[byte 5, 6, 7, 8])
      check w2.finished()
      check f.seekFuts.len == 0
      await w2

      await f.closeWait() # nothing to drain; succeeds with an empty seekFuts
      check readFile(path) == "\x01\x02\x03\x04\x05\x06\x07\x08"
      removeFile(path)

    asyncTest "trackSeekFut swap-removes each settled op in O(1), keeping seekFuts consistent":
      # `trackSeekFut`'s settle callback excises its own entry by the index the
      # `SeekFut` carries — kept current as siblings swap-remove — instead of an
      # O(n) identity scan, so settling N concurrent positioned ops is O(n), not
      # O(n^2). This pins the index bookkeeping the optimisation introduced:
      # completing many tracked ops in a scrambled order must leave `seekFuts`
      # holding exactly the still-pending ops at every step (each entry's `idx`
      # matching its own slot), and empty once all have settled — nothing lost,
      # duplicated, or left dangling at a stale index. The bookkeeping is pure
      # (no kernel op), so this runs even on the sync fallback — no ring needed.
      let f = AsyncFile() # inert container: trackSeekFut only touches f.seekFuts
      var futs: seq[Future[int]] = @[]
      for i in 0 ..< 6:
        let fut = newFuture[int]("test.trackSeekFut")
        trackSeekFut(f, fut)
        futs.add(fut)
      check f.seekFuts.len == 6

      # Settle out of order so a moved entry later removes itself by its *updated*
      # idx — the exact path a naive fixed-index scheme would corrupt.
      var settled = 0
      for idx in [5, 2, 0, 4, 1, 3]:
        futs[idx].complete(idx)
        await sleepAsync(0.milliseconds) # let the settle callback run a poll
        inc settled
        # The live set is exactly the not-yet-settled ops...
        check f.seekFuts.len == futs.len - settled
        for s in f.seekFuts:
          check not s.fut.finished() # no settled op lingers in the set
        # ...and every entry's recorded index still points at its own slot.
        for j in 0 ..< f.seekFuts.len:
          check f.seekFuts[j].idx == j
      check f.seekFuts.len == 0 # all drained, nothing lost or duplicated

  asyncTest "the offset guard is a no-op on non-seekable fds":
    let path = tempPath(".fifo")
    check mkfifo(cstring(path), Mode(0o600)) == 0
    let r = openAsync(path, fmRead)
    let w = openAsync(path, fmWrite)

    # acquire/release never flip the flag for a non-seekable fd...
    acquireOffsetGuard(r)
    check r.seekOpInFlight == false
    releaseOffsetGuard(r)
    check r.seekOpInFlight == false

    # ...and real I/O still works (it uses the readFut/writeFut guards instead).
    await w.write(@[byte 1, 2, 3])
    check (await r.read(3)) == @[byte 1, 2, 3]

    r.close()
    w.close()
    removeFile(path)
