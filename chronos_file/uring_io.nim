## io_uring backend seam (opt-in via `-d:chronosFileUring`; Linux-only).
##
## Every seekable read/write funnels through `readSeekable`/`writeSeekable` (see
## `posix_handle`) and `flush` through `uringFsyncSeam`. When compiled in *and*
## the kernel supports io_uring these shims submit via iori's bridge; otherwise
## the library keeps the synchronous `pread`/`pwrite`/`fsync` path. With the
## define off (the default / every non-Linux target) this module is just the
## `uringCompiled = false` stub and pulls in no dependency on iori.
##
## **Buffer ownership under cancellation** — the low-level `*Buffer*` contract.
## The kernel writes into the caller's buffer until the CQE arrives, so a
## cancelled op must *drain* before the buffer is released. `wireDrain` does this:
## the public future carries `OwnCancelSchedule` (chronos won't force-finish it on
## cancel), so our cancel callback issues an ASYNC_CANCEL and holds the future
## pending until the real CQE is reaped, only then settling it `Cancelled`. The
## caller stays blocked in `cancelAndWait` for that window, which keeps its buffer
## alive — zero-copy, no use-after-free. (The high-level owning API needs no
## caller-side rule: its buffer lives in the awaiting frame, kept alive the same way.)

when defined(chronosFileUring) and defined(linux):
  import pkg/chronos
  import iori

  import common

  const uringCompiled* = true

  # iori registers its completion eventfd with the *calling thread's* dispatcher,
  # so the ring is thread-local, created on first use. A setup failure (kernel
  # < 5.6, io_uring disabled by seccomp/sysctl, ENOMEM, ...) latches `gRing = nil`
  # so we probe once and then fall back to the synchronous path for the thread.
  var gRing {.threadvar.}: UringFileIO
  var gRingProbed {.threadvar.}: bool

  proc uringInstance*(): UringFileIO {.raises: [].} =
    ## The calling thread's ring, or `nil` when the backend is unusable here. First
    ## call lazily creates it (latching any failure so the probe runs once). The
    ## seams resolve it once per op and thread the result in, rather than each layer
    ## re-loading the threadvar.
    if not gRingProbed:
      gRingProbed = true
      gRing =
        try:
          newUringFileIO()
        except CatchableError:
          nil
    gRing

  proc uringAvailable*(): bool {.raises: [].} =
    ## True when the backend is compiled in and usable on this thread (lazily
    ## creates the per-thread ring on first call).
    not uringInstance().isNil

  proc failBridge(retFut: Future[int], err: ref CatchableError, context: string) =
    ## Settle `retFut` with an iori queue-time exception (op could not even be
    ## submitted: closed ring / full SQ) mapped onto the public hierarchy. A raw
    ## future must be `fail`-ed, not `raise`-d past, so this shares
    ## `common.toAsyncFileError` with `mapUringErrors`.
    retFut.fail(toAsyncFileError(err, context))

  proc wireDrain(
      retFut: Future[int],
      ioFut: Future[int32],
      u: UringFileIO,
      context: string,
      zeroIsError = false,
  ) =
    ## Drive `retFut` from the iori bridge future `ioFut` with drain-on-cancel (see
    ## the module header). `retFut` must carry `OwnCancelSchedule` so chronos leaves
    ## its settling to us. `zeroIsError` is threaded to `uringResult` so the write
    ## seam rejects a 0-byte write of a non-empty request as `EIO`.
    var cancelling = false

    proc onDone(udata: pointer) {.gcsafe, raises: [].} =
      if retFut.finished():
        return
      if cancelling:
        # A cancel is in flight, so the caller is blocked in `cancelAndWait` and is
        # owed `CancelledError`; however `ioFut` settled (our `-ECANCELED`, a natural
        # value, or a teardown failure), the kernel is now done with the buffer.
        # Must precede the `failed`/`cancelled` branches, which would otherwise win
        # and leak an `AsyncFileError` to the awaiter.
        retFut.cancelAndSchedule()
        return
      if ioFut.failed():
        failBridge(retFut, ioFut.error, context)
      elif ioFut.cancelled():
        # Unreachable: we abort via `uringCancel` (a `-125` *value*), never a
        # chronos-cancel of `ioFut`. Defensive only — `onDone` is `raises: []`, and
        # falling to the `else` would read a value-less future and crash.
        retFut.cancelAndSchedule()
      else:
        try:
          retFut.complete(uringResult(ioFut.value, context, zeroIsError))
        except AsyncFileOsError as e:
          retFut.fail(e)

    ioFut.addCallback(onDone)

    proc onCancel(udata: pointer) {.gcsafe, raises: [].} =
      if cancelling:
        # chronos re-invokes this every tick while `retFut` stays pending. Once the
        # ASYNC_CANCEL is in flight, issue it no more — re-issuing each tick floods
        # the SQ with redundant cancels and can starve a legitimate submit.
        return
      # Abort via the *public* `uringCancel` (completes `ioFut` with `-ECANCELED`
      # once the CQE drains), deliberately not a chronos-cancel of `ioFut`: that
      # would run iori's external-cancel handler and hide the drain from `onDone`.
      #
      # `uringCancel` does not raise; when the cancel can't be issued now (SQ full /
      # chain open / ring torn down) it returns an already-failed future. So latch
      # `cancelling` only once the cancel is truly in flight — latching first would
      # forfeit it forever (the guard above swallows every retry) and the op could
      # drain only by natural completion (an unbounded wait on stuck storage). On a
      # failed-now cancel, consume the error and leave `cancelling` false to retry
      # next tick; if the op completes first, `onDone` settles `retFut`. The buffer
      # stays alive throughout, since the caller is blocked in `cancelAndWait`.
      let cancelFut =
        try:
          uringCancel(u, ioFut)
        except CatchableError:
          return # not documented to raise; retry next tick
      if cancelFut.failed():
        discard cancelFut.error # consume the failure; leave cancelling false
        return
      cancelling = true

    retFut.cancelCallback = onCancel

  proc trackSeekFut*(f: AsyncFile, fut: Future[int]) =
    ## Record `fut` in the file's in-flight seekable set so `close`/`closeWait` can
    ## drain *all* of them (see `AsyncFileObj.seekFuts`), arming a settle callback
    ## that removes this op's entry. Via `addTracked`/`removeTracked` (in `common`),
    ## which keep removal O(1) so a batch settle is O(n) not O(n^2) (see `SeekFut`).
    ## io_uring path only, so the synchronous backend carries no tracking cost.
    let entry = addTracked(f.seekFuts, fut)
    fut.addCallback(
      proc(udata: pointer) {.gcsafe, raises: [].} =
        # Fires once while `entry` is tracked, so its index is a live slot.
        removeTracked(f.seekFuts, entry)
    )

  proc clampLen(size: int): uint32 {.inline.} =
    # Seam is only entered with size > 0. A single op > 4 GiB is clamped to uint32;
    # the partial-write loop / short-read contract picks up the remainder. `min` is
    # in the uint64 domain so the bound stays representable on a 32-bit target.
    uint32(min(size.uint64, high(uint32).uint64))

  proc wireRwSeam(
      retFut: Future[int],
      u: UringFileIO,
      submit: proc(
        u: UringFileIO,
        fd: cint,
        buf: pointer,
        size: uint32,
        offset: uint64,
        bufRef: ref seq[byte],
      ): Future[int32] {.gcsafe, raises: [CatchableError].},
      fd: cint,
      buf: pointer,
      size: int,
      offset: int64,
      context: string,
      zeroIsError: bool,
  ) =
    ## Shared tail of the read/write seams: submit one op via `submit` into the
    ## caller-created `retFut` (which must carry `OwnCancelSchedule`) and wire
    ## drain-on-cancel; a queue-time failure settles via `failBridge` instead. The
    ## seams differ only in `submit` and `zeroIsError`. `buf` is the caller's
    ## (zero-copy), so iori's `bufRef` is `nil` — the drain keeps it alive. `retFut`
    ## is created in the seam so chronos stamps it with the seam's `raises` set; `u`
    ## is the caller-resolved ring, passed in to avoid re-probing the threadvar.
    let ioFut =
      try:
        # `offset` is non-negative here: the seam rejects `offset < 0` before
        # dispatching to either backend, which is what makes `uint64(offset)` safe —
        # io_uring reads `(u64)-1` as "current file position", so a leaked negative
        # offset would silently diverge from `pread`/`pwrite`.
        submit(u, fd, buf, clampLen(size), uint64(offset), nil)
      except CatchableError as e:
        failBridge(retFut, e, context)
        return
    wireDrain(retFut, ioFut, u, context, zeroIsError)

  proc uringReadSeam*(
      u: UringFileIO, fd: cint, buf: pointer, size: int, offset: int64, context: string
  ): Future[int] {.async: (raw: true, raises: [AsyncFileError, CancelledError]).} =
    ## Submit one io_uring read of up to `size` bytes into `buf` at `offset`,
    ## drain-on-cancel. `buf` is the caller's (zero-copy); `u` is caller-resolved.
    let retFut =
      newFuture[int]("chronos_file.uringRead", {FutureFlag.OwnCancelSchedule})
    wireRwSeam(
      retFut, u, uringRead, fd, buf, size, offset, context, zeroIsError = false
    )
    return retFut

  proc uringWriteSeam*(
      u: UringFileIO, fd: cint, buf: pointer, size: int, offset: int64, context: string
  ): Future[int] {.async: (raw: true, raises: [AsyncFileError, CancelledError]).} =
    ## Submit one io_uring write of up to `size` bytes from `buf` at `offset`,
    ## drain-on-cancel (see `uringReadSeam`).
    ##
    ## `zeroIsError = true`: a 0-byte CQE for this non-empty write becomes `EIO`
    ## (matching `doPwrite`/`doWrite`) so the partial-write loop can't spin forever.
    let retFut =
      newFuture[int]("chronos_file.uringWrite", {FutureFlag.OwnCancelSchedule})
    wireRwSeam(
      retFut, u, uringWrite, fd, buf, size, offset, context, zeroIsError = true
    )
    return retFut

  proc uringFsyncSeam*(
      u: UringFileIO, fd: cint, dataOnly: bool, context: string
  ): Future[int] {.async: (raises: [AsyncFileError, CancelledError]).} =
    ## Submit an io_uring fsync/fdatasync. No buffer, so no drain-on-cancel; `flush`
    ## wraps this in `noCancel` (an fsync can't be meaningfully aborted). Returns the
    ## CQE result via `uringResult` (0 on success). iori's bridge future is untyped,
    ## so `mapUringErrors` is the total handler narrowing `raises: [CatchableError]`
    ## to the seam's `[AsyncFileError, CancelledError]`.
    let res = mapUringErrors(context):
      await uringFsync(u, fd, dataOnly)
    return uringResult(res, context)

else:
  const uringCompiled* = false

  proc uringAvailable*(): bool {.raises: [].} =
    ## io_uring backend not compiled in (`-d:chronosFileUring` off or non-Linux).
    false
