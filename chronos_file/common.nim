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
  SeekFut* = ref object
    ## One tracked in-flight io_uring *seekable* seam op (see `addTracked`). Wraps
    ## the seam future with `idx`, its *current* slot in `AsyncFileObj.seekFuts`.
    ## `addTracked`/`removeTracked` are the seq's only mutators and keep `idx` in
    ## sync as siblings swap-remove, so a settling op excises its own entry in O(1)
    ## by `idx` rather than an O(n) scan — a batch settle is O(n), not O(n^2). Never
    ## nil and never holds a nil `fut` (created only by `addTracked`, dropped only by
    ## `removeTracked`), so consumers (`isInflight`/`closeWait`) need no nil guard.
    fut*: FutureBase
    idx*: int

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
    seekFuts*: seq[SeekFut]
      ## In-flight io_uring *seekable* seam futures (populated only under
      ## `-d:chronosFileUring`; always empty on the synchronous backend). Tracks
      ## *every* suspended seekable read/write so `closeWait` can drain them all
      ## before closing the fd — the seekable analog of `readFut`/`writeFut`. Mutated
      ## only via `addTracked`/`removeTracked` (O(1) removal; see `SeekFut`).
      ## Implicit-offset ops are single-in-flight (`seekOpInFlight`), but concurrent
      ## positioned `*At` ops take no slot, so each gets its own entry and `closeWait`
      ## drains all of them. Synchronous `close`/`=destroy` cannot await a drain, so a
      ## handle with an op in flight must be closed with `closeWait` (see `closeImpl`).
    seekOpInFlight*: bool
      ## Single in-flight slot for the *seekable* implicit-offset family
      ## (`read`/`write`/`readLine` and compound forms, which share `offset`):
      ## while held, a second such op raises `AsyncFileBusyError`. Held across one
      ## whole logical op (every chunk of a multi-chunk read), so it is atomic.
      ## Positioned reads (`readAt`/`readBufferAt`) ignore it; positioned writes
      ## and positioning ops are turned away via `checkOffsetIdle`. No-op for
      ## non-seekable fds (they use `readFut`/`writeFut`). Set and cleared within
      ## one synchronous run today; load-bearing once the seam suspends (io_uring).
      ## See `acquireOffsetGuard`.
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
    ## Raised when a second concurrent op collides with one in flight on the same
    ## handle:
    ## - **Non-seekable fd** (pipe/FIFO/tty): a second `read` (or `write`) while
    ##   one waits on the descriptor. Read and write are tracked separately, so
    ##   one of each may be in flight at once.
    ## - **Seekable file** (regular file / block device): a second implicit-offset
    ##   op (`read`/`write`/`readLine` and compound forms) while one is in flight;
    ##   they share a single slot because they all mutate `offset`. For concurrent
    ##   reads use `readAt`/`readBufferAt` (offset-independent, never rejected).
    ##   Positioned writes (`writeAt`/`writeBufferAt`) and positioning ops
    ##   (`setFilePos`/`setFileSize`) also raise this while a slot is held, since
    ##   they must drop the read-ahead (touching `offset`).
    ##
    ## Subtype of `AsyncFileError`, so `except AsyncFileError` still catches it.

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

template failedFuture*(name: string, err: ref AsyncFileError): untyped {.dirty.} =
  ## A fresh `int` seam future (named `name`) already failed with `err`: the
  ## allocate / settle / return idiom the raw-future seams reject ops with, folded
  ## into one place.
  ##
  ## `{.dirty.}` on purpose: chronos' `newFuture` keys off
  ## `InternalRaisesFutureRaises`, a type the `async` macro injects into the
  ## *calling* proc's body, so only a dirty template resolves it (and `newFuture`/
  ## `fail`) in the expansion scope — hence this must be spliced into a raw-async
  ## proc body, and the `untyped` result carries that proc's exact future type.
  ## `name` reaches `newFuture` as the `static string` it needs because the template
  ## substitutes the literal.
  block:
    let rejectedFut = newFuture[int](name)
    rejectedFut.fail(err)
    rejectedFut

template cancelledFuture*(name: string): untyped {.dirty.} =
  ## A fresh `int` seam future (named `name`) already scheduled for cancellation:
  ## the seam's "reject an in-flight op's chunk while `closeWait` drains" outcome.
  ## `cancelAndSchedule` because chronos forbids `fail`-ing with `CancelledError`.
  ## `{.dirty.}` for the same reason as `failedFuture` (see it).
  block:
    let cancelledFut = newFuture[int](name)
    cancelledFut.cancelAndSchedule()
    cancelledFut

# io_uring (iori) error boundary
# The seekable seam awaits iori bridge futures of type `Future[int32]`, whose
# failures must never escape the public API. Funneled through here:
#   * a CQE result `< 0` is `-errno` -> `AsyncFileOsError` (carrying the
#     `OSErrorCode`), matching the `doPread`/`doPwrite` contract;
#   * a raw `IOError` (ring closed / SQ full) or `OSError` thrown when the op
#     cannot even be queued -> `AsyncFileError` / `AsyncFileOsError`.
# Deliberately free of any iori import (uses stdlib `IOError`/`OSError`), so it
# compiles and is tested independently. If iori later reports typed errors this
# collapses to just `uringResult`.

proc uringResult*(
    res: int32, context = "", zeroIsError = false
): int {.raises: [AsyncFileOsError].} =
  ## Translate an io_uring CQE result into the contract: a negative value is
  ## `-errno`, raised as `AsyncFileOsError` (preserving the `OSErrorCode`); a
  ## non-negative value is the byte count, returned unchanged. `context` prefixes
  ## the message like `doPread`/`doPwrite`. The single place the `res < 0` rule lives.
  ##
  ## `zeroIsError` mirrors `doPwrite`/`doWrite` for the *write* seam: a 0-byte write
  ## of a non-empty request can't make progress, so the caller's partial-write loop
  ## would spin forever re-submitting it — turn it into `EIO`. Reads (0 = EOF) and
  ## fsync (0 = success) leave it `false`.
  ##
  ## Cancellation is not decoded here: a `-ECANCELED` is only how iori settles its
  ## own bridge future; the seam drives chronos cancellation and keeps the public
  ## future pending, so it never reaches this mapping in normal flow.
  if res < 0:
    # Negate in `int`, not `int32`: `-res` would overflow for `res == low(int32)`
    # (`OverflowDefect`). This only defends the *negation*; the `OSErrorCode`
    # (int32-backed) conversion below would still `RangeDefect` on that same
    # `low(int32)`, since `2147483648` does not fit. That is acceptable because the
    # input is unreachable: every real `-errno` is small and fits an `OSErrorCode`.
    raise newAsyncFileOsError(OSErrorCode(-res.int), context)
  elif res == 0 and zeroIsError:
    raise newAsyncFileOsError(oserrno.EIO, context)
  int(res)

proc toAsyncFileError*(err: ref CatchableError, context = ""): ref AsyncFileError =
  ## Map an iori-side *exception* onto the public hierarchy and *return* it (never
  ## raises), so a caller that cannot `raise` past its control flow can settle a raw
  ## future with it (`uring_io.failBridge`); `mapUringErrors` re-raises it. The
  ## exception-shaped sibling of `uringResult`.
  ##   * an in-contract `AsyncFileError` (e.g. raised by `uringResult` in the body)
  ##     passes through unchanged — never double-wrapped nor downgraded. Checked
  ##     first; its order vs the `OSError` branch is immaterial (never both match).
  ##   * `OSError` -> `AsyncFileOsError`, `errorCode` preserved (newAsyncFileOsError
  ##     prefixes `context`).
  ##   * anything else (iori's ring-lifecycle `IOError` for closed ring / full SQ,
  ##     plus residual) -> a bare `AsyncFileError`, with `context` prefixed.
  ##
  ## `CancelledError` is not handled here: cancellation is the seam's to drive, so
  ## `mapUringErrors` re-raises it first and `failBridge` only sees a non-cancel
  ## failure.
  if err of AsyncFileError:
    (ref AsyncFileError)(err)
  elif err of OSError:
    newAsyncFileOsError(OSErrorCode((ref OSError)(err).errorCode), context)
  else:
    let prefix =
      if context.len > 0:
        context & ": "
      else:
        ""
    newAsyncFileError(prefix & err.msg)

template mapUringErrors*(context: string, body: untyped): untyped =
  ## Run `body` (typically `await` on an iori bridge future + `uringResult`) with
  ## iori's failure types translated into the public hierarchy. The handler is
  ## *total*: every `CatchableError` is narrowed to `AsyncFileError`/`CancelledError`,
  ## which is what lets an `await` on iori's untyped `Future[int32]` (inferred
  ## `raises: [CatchableError]`) satisfy a seam's
  ## `raises: [AsyncFileError, CancelledError]` — see `uringFsyncSeam`.
  ##   * `CancelledError` re-raised unchanged (the seam's to drive).
  ##   * everything else mapped by `toAsyncFileError` and re-raised — the same
  ##     mapping `failBridge` uses.
  ##
  ## SQ-full backpressure is iori's job, not a retry loop here: matching the message
  ## string would be brittle and reorder submissions / break linked chains.
  try:
    body
  except CancelledError as cancelErr:
    raise cancelErr
  except CatchableError as err:
    raise toAsyncFileError(err, context)

proc addTracked*(seekFuts: var seq[SeekFut], fut: FutureBase): SeekFut =
  ## Append `fut` and return its `SeekFut` entry, recording its slot index so
  ## `removeTracked` can excise it in O(1). These two are the *only* mutators of a
  ## `seekFuts` seq — what keeps the `idx`-equals-slot invariant (see `SeekFut`). The
  ## caller arms `entry`'s settle callback to call `removeTracked` (see `trackSeekFut`).
  result = SeekFut(fut: fut, idx: seekFuts.len)
  seekFuts.add(result)

proc removeTracked*(seekFuts: var seq[SeekFut], entry: SeekFut) =
  ## Swap-remove `entry` in O(1) by its carried index: `swap` with the tail (no
  ## refcount churn), fix the moved entry's `idx`, drop the tail. `entry` must still
  ## be tracked — its settle callback fires once while it is, so `idx` is a live slot.
  ## The `doAssert` turns any violation (a third mutator, a double-track, a stale
  ## callback) into a loud failure instead of a silent out-of-bounds write, and
  ## survives `-d:danger` like the `=destroy` UAF net.
  let i = entry.idx
  let last = seekFuts.high
  doAssert i in 0 .. last,
    "SeekFut.idx out of sync with seekFuts (len " & $seekFuts.len & ")"
  if i != last:
    swap(seekFuts[i], seekFuts[last])
    seekFuts[i].idx = i
  seekFuts.setLen(last)

proc isInflight*(s: SeekFut): bool {.inline.} =
  ## A tracked seekable op is in flight iff its seam future is still suspended. `s`/
  ## `s.fut` are never nil (see `SeekFut`), so this is just the finished test — the
  ## per-entry liveness predicate both teardown paths share (`hasInflightSeekOp` and
  ## the `closeWait` drain) so they cannot drift.
  not s.fut.finished()

proc hasInflightSeekOp*(seekFuts: seq[SeekFut]): bool =
  ## True iff any op in `seekFuts` is still suspended (`isInflight`). The single
  ## condition both teardown paths reason about, shared so they cannot diverge:
  ##   * `closeImpl` **raises** `AsyncFileError` — synchronous `close` can't await
  ##     the drain, and closing the fd under a live op risks a deferred-submit
  ##     fd-reuse / a write into a since-released buffer.
  ##   * `=destroy` **asserts** it is false — likewise can't await, and by the
  ##     reachability invariant (iori roots the bridge future until the CQE is
  ##     reaped) it only runs once every tracked op has settled.
  ## Trivially false on the synchronous backend, where `seekFuts` stays empty.
  for s in seekFuts:
    if s.isInflight():
      return true

proc `=destroy`(f: AsyncFileObj) =
  ## Best-effort safety net for a forgotten `close`: release the fd (and, for
  ## non-seekable fds, its dispatcher registration). Prefer `close`/`closeWait`.
  ## While an op is in flight the handle stays reachable, so this won't run until it
  ## settles (a pipe/FIFO read/write is held by the dispatcher's readiness callback;
  ## an io_uring seekable op by its `seekFuts` settle callback, via iori's bridge
  ## future — both capture this handle). Don't depend on that ordering, though: the
  ## destructor can neither fail nor cancel a pending future, so rather than trust
  ## the invariant silently it **asserts** no io_uring seekable op is in flight
  ## (`hasInflightSeekOp`), mirroring `closeImpl` which raises on the same state.
  ##
  ## The dispatcher registration is thread-local, so this is only safe when the
  ## destructor runs on the dispatcher-owning thread while that dispatcher is alive.
  ## A handle dropped on another thread or after loop shutdown is outside that
  ## window — one more reason to close explicitly.
  ##
  ## Only acts on a handle a real constructor opened: a default `AsyncFile()` has
  ## `opened == false` and owns no fd, so it must not touch fd 0 or unregister an
  ## fd that was never registered (which would abort in the dispatcher).
  if f.opened and not f.closed:
    # No tracked io_uring op can be in flight here (reachability invariant above).
    # Assert rather than trust silently: were it broken (ring torn down, reachability
    # lost), closing the fd and freeing the buffers below would be a UAF under a live
    # kernel op. `doAssert` survives `-d:danger` (the point of a UAF net); the check
    # is once-per-handle and empty on the synchronous backend.
    doAssert not hasInflightSeekOp(f.seekFuts),
      "=destroy ran with an io_uring seekable op still in flight; the handle was " &
        "collected under a live kernel op — close such handles with closeWait"

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
    `=destroy`(f.seekFuts)
