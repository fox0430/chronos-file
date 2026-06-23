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

# io_uring (iori) error boundary
# Groundwork for the planned io_uring backend. The seekable seam in
# `posix_handle` will later `await` iori bridge futures of type
# `Future[int32]`, which report failures in two shapes that must never escape
# chronos-file's public API. Both are funneled through here:
#   * a CQE result `< 0` is `-errno` and becomes `AsyncFileOsError` (carrying the
#     `OSErrorCode`), matching the `doPread`/`doPwrite` contract;
#   * a raw `IOError` (ring closed / SQ full) or `OSError` thrown when the op
#     cannot even be queued becomes `AsyncFileError` / `AsyncFileOsError`.
# Kept next to the error constructors and deliberately free of any iori import,
# so it compiles and is tested before iori is a dependency (`IOError`/`OSError`
# are stdlib types). If iori later reports typed errors the wrapper collapses to
# just `uringResult` — but that is an iori-side change and is out of scope here.

proc uringResult*(res: int32, context = ""): int {.raises: [AsyncFileOsError].} =
  ## Translate an io_uring CQE result into chronos-file's contract: a negative
  ## value is `-errno` and is raised as `AsyncFileOsError` (preserving the
  ## `OSErrorCode`); a non-negative value is the byte count and is returned
  ## unchanged. `context` prefixes the message with the operation name, exactly
  ## as `doPread`/`doPwrite` do. Covers every CQE result iori can produce (an
  ## `-errno`, which always fits an `OSErrorCode`, or a byte count) and is the
  ## single place the `res < 0 -> newAsyncFileOsError(OSErrorCode(-res))` rule
  ## lives.
  ##
  ## Cancellation is *not* decoded here: a low-level `-ECANCELED` is only how
  ## iori settles its own bridge future, and the seam keeps the public future
  ## pending and drives chronos cancellation instead, so it does not reach
  ## this mapping in normal flow.
  if res < 0:
    # Negate in `int`, not `int32`: `-res` would overflow for `res == low(int32)`
    # (`OverflowDefect`). Every real `-errno` is small and fits an `OSErrorCode`;
    # widening just keeps the arithmetic defensive for an unreachable input.
    raise newAsyncFileOsError(OSErrorCode(-res.int), context)
  int(res)

template mapUringErrors*(context: string, body: untyped): untyped =
  ## Run `body` (typically an `await` on an iori bridge future followed by
  ## `uringResult` decoding) with iori's internal failure types translated into
  ## chronos-file's public hierarchy, so neither `IOError` nor `OSError` ever
  ## leaks past the public API:
  ##   * `OSError` -> `AsyncFileOsError` (its `errorCode` becomes the OSErrorCode);
  ##   * `IOError`  -> `AsyncFileError` (no errno: an internal-state failure such
  ##     as a closed ring or a full submission queue).
  ## `AsyncFileError` (already in-contract, e.g. raised by `uringResult` inside
  ## `body`) and `CancelledError` propagate untouched — they are caught by
  ## neither branch, so this layer leaves the seam's cancellation handling alone.
  ##
  ## SQ-full backpressure is iori's responsibility, not a retry loop here:
  ## matching on the message string would be brittle, would reorder submissions
  ## and break linked chains. This layer only guarantees the failure is surfaced
  ## in-contract.
  try:
    body
  except OSError as osErr:
    raise newAsyncFileOsError(OSErrorCode(osErr.errorCode), context)
  except IOError as ioErr:
    # Bind `context` once: it is a typed template parameter, so each textual use
    # re-evaluates the argument expression (`context.len` and `context & ": "`
    # would evaluate it twice).
    let ctx = context
    let prefix =
      if ctx.len > 0:
        ctx & ": "
      else:
        ""
    raise newAsyncFileError(prefix & ioErr.msg)

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
