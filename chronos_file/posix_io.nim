## The read/write surface: low-level `readBuffer`/`writeBuffer`, the high-level
## `read`/`readAll`/`readLine`/`write` family and the positioned `*At` procs.

import std/posix

import pkg/chronos
import pkg/chronos/[osutils, oserrno]

import common, posix_handle

{.push raises: [].}

proc readBufferSeekable(
    f: AsyncFile, buf: pointer, size: int, alreadyGuarded: bool
): Future[int] {.async: (raises: [AsyncFileError, CancelledError]).} =
  ## Seekable branch of `readBuffer`: drop any `readLine` read-ahead, read at the
  ## tracked `f.offset` through the `readSeekable` seam, then advance the offset.
  ## Split out so the raw `readBuffer` can `return` this future for seekable fds
  ## while keeping its non-seekable EAGAIN/`addReader2` machinery inline. The seam
  ## completes synchronously, so this never suspends or touches the dispatcher.
  ##
  ## Single-in-flight guard: a standalone read (`alreadyGuarded = false`) takes
  ## and releases the `offset` slot itself; a multi-chunk caller holds it once
  ## around its whole loop and passes true (see `withOffsetGuard`).
  withOffsetGuard(f, alreadyGuarded):
    reconcile(f) # drop any readLine read-ahead so this read sees the logical pos
    let n = await readSeekable(f, buf, size, f.offset, "read")
    f.offset.inc(n)
    return n

proc writeBufferSeekable(
    f: AsyncFile, buf: pointer, size: int
): Future[void] {.async: (raises: [AsyncFileError, CancelledError]).} =
  ## Seekable branch of `writeBuffer`: drop any `readLine` read-ahead, then write
  ## the whole buffer through the append-aware `writeSeekable` seam one chunk at a
  ## time, advancing `f.offset` per successful chunk so a mid-write error leaves
  ## it consistent with the bytes actually written. Split out so the raw
  ## `writeBuffer` can `return` this future for seekable fds while keeping its
  ## non-seekable EAGAIN/`addWriter2` machinery inline. The seam completes
  ## synchronously, so this never suspends or touches the dispatcher.
  ##
  ## Takes the single-in-flight `offset` slot once, held across the whole
  ## partial-write loop, so a concurrent implicit-offset op is rejected with
  ## `AsyncFileBusyError` (see `withOffsetGuard`).
  withOffsetGuard(f):
    reconcile(f) # drop any readLine read-ahead so this write sees the logical pos
    var written = 0
    while written < size:
      let p = cast[pointer](cast[uint](buf) + uint(written))
      let res = await writeSeekable(f, p, size - written, f.offset, "write")
      written.inc(res)
      f.offset.inc(res)

proc readBufferImpl(
    f: AsyncFile, buf: pointer, size: int, alreadyGuarded: bool
): Future[int] {.async: (raw: true, raises: [AsyncFileError, CancelledError]).} =
  ## Shared body of `readBuffer` (see it for the public contract). `alreadyGuarded`
  ## is true when a multi-chunk caller (`readAll`/`readExactly` via `readInto`)
  ## already holds the single-in-flight `offset` slot for the whole logical read,
  ## so the seekable leaf must not re-take it; a standalone read passes false and
  ## the leaf takes the slot for its one chunk.
  let uerr = usabilityError(f)

  # Fast path: a usable, non-empty seekable read needs none of the raw-future
  # machinery below, so delegate straight to the seam helper, which returns its
  # own future. This keeps the regular-file hot path to a single future
  # allocation (the helper's) instead of also allocating an unused `retFuture`
  # here. Every other case (errors, empty reads, non-seekable fds) falls through
  # and uses `retFuture`.
  if uerr.isNil and size > 0 and f.seekable:
    # Regular files / block devices: read at the tracked offset through the
    # seekable seam. Never returns EAGAIN, so it completes synchronously without
    # touching the dispatcher or the kernel file position.
    return readBufferSeekable(f, buf, size, alreadyGuarded)

  let retFuture = newFuture[int]("chronos_file.readBuffer")
  if not uerr.isNil:
    retFuture.fail(uerr)
    return retFuture
  if size < 0:
    # Reject instead of passing the value through: a negative size would wrap
    # to a huge csize_t in the syscall (pread fails with EFAULT, write loops
    # would silently no-op), so the contract is made explicit here.
    retFuture.fail(newAsyncFileError("negative read size: " & $size))
    return retFuture
  if size == 0:
    retFuture.complete(0)
    return retFuture

  # Only non-seekable fds (pipe/FIFO/tty) reach here: the fast path took every
  # usable non-empty seekable read, and the guards above handled the seekable
  # error/empty cases.
  if not f.readFut.isNil and not f.readFut.finished():
    # Only one read may wait on the descriptor at a time; a second concurrent
    # read would clobber the reader registration and `f.readFut` tracking.
    retFuture.fail(newAsyncFileBusyError("read already in progress"))
    return retFuture

  proc tryRead(): bool {.gcsafe, raises: [].} =
    ## true  = settled (completed or failed)
    ## false = EAGAIN/EWOULDBLOCK, keep watching the descriptor
    if retFuture.finished():
      # Defensive: a stale readiness callback must never complete twice.
      return true
    let res = handleEintr(posix.read(cint(f.fd), buf, size))
    if res < 0:
      let err = osLastError()
      if err == oserrno.EAGAIN or err == oserrno.EWOULDBLOCK:
        return false
      retFuture.fail(newAsyncFileOsError(err, "read"))
      return true
    elif res == 0:
      retFuture.complete(0)
      return true
    else:
      f.offset.inc(res)
      retFuture.complete(res)
      return true

  if not tryRead():
    # Reached only for descriptors that can return EAGAIN (pipe/FIFO/tty).
    # Regular files never get here, so EPERM from epoll_ctl is never triggered.
    proc readCb(udata: pointer) {.gcsafe, raises: [].} =
      if retFuture.finished():
        # Already settled elsewhere: `close` failed it (and removed the reader),
        # or cancellation ran the cancel callback (likewise). The reader is gone
        # and `f.fd` may have been closed and its number reused, so do not touch
        # it again. Mirrors the `not finished()` guard in the cancel callback.
        return
      if tryRead():
        discard removeReader2(f.fd)
        f.readFut = nil

    let res = addReader2(f.fd, readCb)
    if res.isErr:
      retFuture.fail(newAsyncFileOsError(res.error, "read"))
    else:
      f.readFut = retFuture

      proc cancel(udata: pointer) {.gcsafe, raises: [].} =
        if not (retFuture.finished()):
          discard removeReader2(f.fd)
        f.readFut = nil

      retFuture.cancelCallback = cancel
  return retFuture

proc readBuffer*(
    f: AsyncFile, buf: pointer, size: int
): Future[int] {.async: (raw: true, raises: [AsyncFileError, CancelledError]).} =
  ## Reads up to `size` bytes into `buf`. Returns the number of bytes read; 0
  ## means end of file. The caller owns `buf` and must keep it alive until the
  ## returned future completes.
  ##
  ## This is the low-level path and bypasses the `readLine` pushback buffer, so
  ## do not interleave `readBuffer` with `readLine` on the same file.
  ##
  ## **Concurrency (seekable files):** an implicit-offset op — at most one of the
  ## family may be in flight; a second raises `AsyncFileBusyError` (see it for the
  ## contract). Use `readBufferAt`/`readAt` for concurrent I/O. A zero-size call
  ## is a no-op, neither taking the slot nor rejected by it.
  return readBufferImpl(f, buf, size, alreadyGuarded = false)

proc writeBuffer*(
    f: AsyncFile, buf: pointer, size: int
): Future[void] {.async: (raw: true, raises: [AsyncFileError, CancelledError]).} =
  ## Writes exactly `size` bytes from `buf`. Handles partial writes. The caller
  ## owns `buf` and must keep it alive until the returned future completes.
  ##
  ## Not atomic on non-seekable fds (pipe/FIFO/tty): if the write suspends on a
  ## full buffer and is then cancelled or fails, the bytes already accepted by
  ## the kernel stay written and `f.offset` reflects that partial progress.
  ##
  ## **Concurrency (seekable files):** an implicit-offset op sharing the single
  ## slot; a concurrent one raises `AsyncFileBusyError` (see it). Use
  ## `writeBufferAt`/`writeAt` for concurrent positioned writes. A zero-size call
  ## is a no-op, neither taking the slot nor rejected by it.
  let uerr = usabilityError(f)

  # Fast path (see readBuffer): a usable, non-empty seekable write delegates
  # straight to the seam helper, which returns its own future, so the
  # regular-file hot path allocates no unused `retFuture` here.
  if uerr.isNil and size > 0 and f.seekable:
    # Regular files / block devices: write at the tracked offset through the
    # seekable seam — pwrite normally, or a sequential write() in append mode so
    # the kernel picks the end of file (atomic append) on every platform (pwrite
    # would only append on Linux and would overwrite at a stale offset on
    # POSIX-conforming platforms). Completes synchronously; the seam is
    # append-aware and the helper advances `f.offset` per successful chunk.
    return writeBufferSeekable(f, buf, size)

  let retFuture = newFuture[void]("chronos_file.writeBuffer")
  if not uerr.isNil:
    retFuture.fail(uerr)
    return retFuture
  if size < 0:
    # See readBuffer: a negative size must not reach the syscall loop (it
    # would complete silently here because `written < size` is never true).
    retFuture.fail(newAsyncFileError("negative write size: " & $size))
    return retFuture
  if size == 0:
    retFuture.complete()
    return retFuture

  # Only non-seekable fds reach here (see readBuffer's fast path).
  if not f.writeFut.isNil and not f.writeFut.finished():
    # Only one write may wait on the descriptor at a time.
    retFuture.fail(newAsyncFileBusyError("write already in progress"))
    return retFuture

  var written = 0

  proc tryWrite(): bool {.gcsafe, raises: [].} =
    ## true  = settled (all bytes written or failed)
    ## false = EAGAIN/EWOULDBLOCK, keep watching the descriptor
    if retFuture.finished():
      # Defensive: a stale readiness callback must never complete twice.
      return true
    while written < size:
      let p = cast[pointer](cast[uint](buf) + uint(written))
      let res = handleEintr(posix.write(cint(f.fd), p, size - written))
      if res < 0:
        let err = osLastError()
        if err == oserrno.EAGAIN or err == oserrno.EWOULDBLOCK:
          return false
        retFuture.fail(newAsyncFileOsError(err, "write"))
        return true
      elif res == 0:
        # write() must not return 0 for a non-empty request; treat the lack of
        # progress as an error instead of spinning the loop forever.
        retFuture.fail(newAsyncFileOsError(oserrno.EIO, "write"))
        return true
      else:
        written.inc(res)
        f.offset.inc(res)
    retFuture.complete()
    return true

  if not tryWrite():
    proc writeCb(udata: pointer) {.gcsafe, raises: [].} =
      if retFuture.finished():
        # Already settled elsewhere (close/cancel removed the writer). `f.fd`
        # may have been closed and its number reused, so do not touch it again.
        # Mirrors the `not finished()` guard in the cancel callback.
        return
      if tryWrite():
        discard removeWriter2(f.fd)
        f.writeFut = nil

    let res = addWriter2(f.fd, writeCb)
    if res.isErr:
      retFuture.fail(newAsyncFileOsError(res.error, "write"))
    else:
      f.writeFut = retFuture

      proc cancel(udata: pointer) {.gcsafe, raises: [].} =
        if not (retFuture.finished()):
          discard removeWriter2(f.fd)
        f.writeFut = nil

      retFuture.cancelCallback = cancel
  return retFuture

proc readInto(
    f: AsyncFile, dst: pointer, size: int, alreadyGuarded: bool
): Future[int] {.async: (raises: [AsyncFileError, CancelledError]).} =
  ## Reads up to `size` bytes into `dst`, serving any `readLine` pushback
  ## first (unlike the low-level `readBuffer`, which bypasses it). Returns the
  ## number of bytes read (0 = end of file). Shared by `read` and
  ## `readExactly`; the caller owns `dst` and must keep it alive until the
  ## returned future completes.
  ##
  ## Never suspends while holding deliverable bytes: once pushback supplied
  ## something, the descriptor is tried at most once without blocking and
  ## EAGAIN/EOF yields a short read.
  ##
  ## `alreadyGuarded` is forwarded to the seekable read leaf; it is mandatory (no
  ## default) so a caller can never silently drop it. Multi-chunk callers pass
  ## true (they hold the slot for the whole loop); single-shot callers pass false.
  ## Inert on the non-seekable and pushback paths (the guard is a no-op there).
  if size <= 0:
    return 0

  if f.pushback.len == 0:
    return await readBufferImpl(f, dst, size, alreadyGuarded)

  # Pushback only ever exists on a non-seekable fd (bare CR in readLine), so
  # the sequential-read top-up below is the right path. Serve it in one batch
  # (front-by-front `delete` would be O(n²)).
  let take = min(size, f.pushback.len)
  copyMem(dst, addr f.pushback[0], take)
  f.pushback = f.pushback[take ..^ 1]
  var have = take
  if have < size and (f.readFut.isNil or f.readFut.finished()):
    # Top up with one non-blocking syscall; EAGAIN/EOF return a short read.
    # Skipped while a read is in flight (the pushback bytes alone are
    # returned; the descriptor is untouched, so no AsyncFileBusyError). No
    # await here, so there is no cancellation point to restore pushback for.
    let p = cast[pointer](cast[uint](dst) + uint(have))
    let res = handleEintr(posix.read(cint(f.fd), p, size - have))
    if res > 0:
      f.offset.inc(res)
      have.inc(res)
    elif res < 0:
      let err = osLastError()
      if err != oserrno.EAGAIN and err != oserrno.EWOULDBLOCK:
        # The caller sees an exception, not `dst`: restore the consumed
        # pushback so no bytes are lost. The first `take` bytes of `dst` are
        # exactly the pushback consumed above, so rebuild from there (only
        # this error path pays the allocation).
        var consumed = newSeq[byte](take)
        copyMem(addr consumed[0], dst, take)
        f.pushback = consumed & f.pushback
        raise newAsyncFileOsError(err, "read")
  return have

proc read*(
    f: AsyncFile, size: int
): Future[seq[byte]] {.async: (raises: [AsyncFileError, CancelledError]).} =
  ## Reads up to `size` bytes and returns them. An empty seq means end of file.
  ## The buffer is owned by the implementation, so cancellation is safe.
  ## Any bytes `readLine` pushed back are served first; a call that got
  ## pushback bytes never suspends (at most one non-blocking syscall, so the
  ## result may be short).
  ##
  ## This issues a single underlying read, so a short read (fewer than `size`
  ## bytes, especially on pipes/FIFOs) does not imply end of file. Use `readAll`
  ## or loop until an empty seq if you need exactly `size` bytes.
  ##
  ## Note: this allocates a `size`-byte buffer up front and shrinks it to the
  ## bytes actually read, so passing a very large `size` reserves that much even
  ## when far fewer bytes arrive.
  ##
  ## **Concurrency (seekable files):** an implicit-offset op sharing the single
  ## slot; a concurrent one raises `AsyncFileBusyError` (see it). Use `readAt` for
  ## concurrency.
  checkOpen(f)

  if size <= 0:
    return newSeq[byte](0)

  var buffer = newSeq[byte](size)
  let n = await readInto(f, addr buffer[0], size, alreadyGuarded = false)
  buffer.setLen(n)
  return buffer

proc readString*(
    f: AsyncFile, size: int
): Future[string] {.async: (raises: [AsyncFileError, CancelledError]).} =
  ## `read` returning a `string` instead of `seq[byte]` — for text, without a
  ## caller-side byte-to-string conversion (mirrors `readAll`/`readAllString`).
  ## Same contract as `read`: up to `size` bytes via a single underlying read
  ## (short reads possible, `readLine` pushback served first), `""` = end of
  ## file, and the `size`-byte buffer is allocated up front.
  checkOpen(f)

  if size <= 0:
    return ""

  var buffer = newString(size)
  let n = await readInto(f, addr buffer[0], size, alreadyGuarded = false)
  buffer.setLen(n)
  return buffer

proc readAll*(
    f: AsyncFile
): Future[seq[byte]] {.async: (raises: [AsyncFileError, CancelledError]).} =
  ## Reads the whole file (from the current position) until end of file.
  ## Reads into a single reused chunk buffer (via `readInto`, which honours the
  ## `readLine` pushback) instead of allocating a fresh seq per iteration; the
  ## result grows geometrically via `add`.
  checkOpen(f) # readInto (unlike read) does not guard the handle itself

  # Hold the offset slot for the whole multi-chunk read so the logical op is
  # atomic; each chunk passes `alreadyGuarded = true` so it does not re-take it.
  withOffsetGuard(f):
    const chunkSize = 64 * 1024
    var data: seq[byte] = @[]
    var chunk = newSeq[byte](chunkSize)
    while true:
      let n = await readInto(f, addr chunk[0], chunkSize, alreadyGuarded = true)
      if n == 0:
        break
      data.add(chunk.toOpenArray(0, n - 1))
    return data

proc readAllString*(
    f: AsyncFile
): Future[string] {.async: (raises: [AsyncFileError, CancelledError]).} =
  ## `readAll` returning a `string` instead of `seq[byte]` — for text files,
  ## without a caller-side byte-to-string conversion. Reads the whole file
  ## (from the current position) until end of file, with the same reused
  ## chunk buffer and pushback handling as `readAll`.
  checkOpen(f) # readInto (unlike read) does not guard the handle itself

  # Hold the single-in-flight offset slot across the whole read (see `readAll`).
  withOffsetGuard(f):
    const chunkSize = 64 * 1024
    var data = ""
    var chunk = newSeq[byte](chunkSize)
    while true:
      let n = await readInto(f, addr chunk[0], chunkSize, alreadyGuarded = true)
      if n == 0:
        break
      let oldLen = data.len
      data.setLen(oldLen + n)
      copyMem(addr data[oldLen], addr chunk[0], n)
    return data

proc readExactly*(
    f: AsyncFile, size: int
): Future[seq[byte]] {.async: (raises: [AsyncFileError, CancelledError]).} =
  ## Reads exactly `size` bytes, looping until the buffer is full. Unlike `read`
  ## — which issues a single underlying syscall and may return fewer bytes (a
  ## short read does not imply EOF) — this keeps reading until `size` bytes have
  ## been collected. Raises `AsyncFileIncompleteError` if end of file is reached
  ## first. `size <= 0` returns an empty seq.
  ##
  ## Reads straight into one pre-allocated `size`-byte buffer (via `readInto`),
  ## so there is no per-iteration seq allocation or copy. Like `read`, it serves
  ## any `readLine` pushback first.
  ##
  ## **Cancellation:** unlike `read` (a single underlying read, fully
  ## cancel-safe), `readExactly` consumes bytes across several reads. On a
  ## non-seekable fd (pipe/FIFO/tty), cancelling after some bytes have already
  ## been read discards them — pipe bytes cannot be un-read, and any `readLine`
  ## pushback consumed by an earlier iteration is not restored. Seekable files
  ## are unaffected: every read completes synchronously, so `readExactly` never
  ## suspends and there is no point at which it can be cancelled mid-way.
  checkOpen(f)

  if size <= 0:
    return newSeq[byte](0)

  # Hold the single-in-flight offset slot across the whole read (see `readAll`).
  withOffsetGuard(f):
    var data = newSeq[byte](size)
    var have = 0
    while have < size:
      let n = await readInto(f, addr data[have], size - have, alreadyGuarded = true)
      if n == 0:
        raise newAsyncFileIncompleteError(
          "end of file after " & $have & " of " & $size & " bytes"
        )
      have.inc(n)
    return data

proc readExactlyString*(
    f: AsyncFile, size: int
): Future[string] {.async: (raises: [AsyncFileError, CancelledError]).} =
  ## `readExactly` returning a `string` instead of `seq[byte]`. Reads exactly
  ## `size` bytes, raising `AsyncFileIncompleteError` if end of file is reached
  ## first; `size <= 0` returns `""`. Shares `readExactly`'s contract, including
  ## its cancellation caveat on non-seekable fds (bytes consumed by earlier
  ## iterations are discarded if cancelled mid-way).
  checkOpen(f)

  if size <= 0:
    return ""

  # Hold the single-in-flight offset slot across the whole read (see `readAll`).
  withOffsetGuard(f):
    var data = newString(size)
    var have = 0
    while have < size:
      let n = await readInto(f, addr data[have], size - have, alreadyGuarded = true)
      if n == 0:
        raise newAsyncFileIncompleteError(
          "end of file after " & $have & " of " & $size & " bytes"
        )
      have.inc(n)
    return data

proc tryReadByte(f: AsyncFile): tuple[got: bool, b: byte] {.raises: [AsyncFileError].} =
  ## Non-blocking single-byte read used only for `readLine`'s CR/LF
  ## disambiguation on a non-seekable fd. Serves a pending pushback byte first
  ## (logically already available), otherwise tries the descriptor exactly once.
  ##
  ## Self-contained "next logical byte" primitive: at its sole call site
  ## (immediately after `readLine` read the CR via `await read(f, 1)`) the
  ## pushback was already drained by that read and no descriptor read is in
  ## flight, so the pushback and `readFut` guards below are never taken today —
  ## they keep the primitive correct if it is ever reused elsewhere.
  ##
  ## Returns `got = false` when no byte is available right now — covering both
  ## end of file *and*, crucially, EAGAIN (the next byte has not arrived yet).
  ## `readLine` must NOT suspend to disambiguate a bare CR from CRLF: on an idle
  ## stream (writer paused awaiting our reply) that would hang an already
  ## complete CR-terminated line until more data or EOF, deadlocking line-based
  ## protocols. So a not-yet-available next byte is reported the same as EOF and
  ## the CR is taken as a bare-CR terminator. This mirrors the seekable path,
  ## which peeks the read-ahead buffer and likewise returns the line when no next
  ## byte follows. Nothing is consumed on `got = false`, so the logical position
  ## is unchanged; on `got = true` `f.offset` is advanced for the byte read.
  if f.pushback.len > 0:
    result = (true, f.pushback[0])
    f.pushback = f.pushback[1 ..^ 1]
    return

  if not f.readFut.isNil and not f.readFut.finished():
    # A descriptor read is parked on the fd; do not steal the byte it is waiting
    # for. Report "no byte now" so the CR is taken as a bare CR (same as EAGAIN),
    # mirroring readInto's top-up guard.
    return (false, 0'u8)

  var b: byte
  let res = handleEintr(posix.read(cint(f.fd), addr b, 1))
  if res > 0:
    f.offset.inc(res)
    (true, b)
  elif res == 0:
    (false, 0'u8) # end of file
  else:
    let err = osLastError()
    if err == oserrno.EAGAIN or err == oserrno.EWOULDBLOCK:
      (false, 0'u8) # next byte not available yet — do not block; treat as bare CR
    else:
      raise newAsyncFileOsError(err, "read")

proc readLine*(
    f: AsyncFile, limit = 0
): Future[Opt[string]] {.async: (raises: [AsyncFileError, CancelledError]).} =
  ## Reads a single line (without the trailing newline). Recognises `\n` and
  ## `\c\L` (and a bare `\c`). Returns `Opt.none(string)` at end of file and
  ## `Opt.some(line)` otherwise — so an empty line (`Opt.some("")`) is
  ## distinguishable from EOF. Precisely: `none` is returned only when the
  ## call consumed no bytes at all; a final line without a trailing
  ## terminator is still `some`.
  ##
  ## `limit` bounds the line length (0 = unlimited): once `limit` bytes have
  ## accumulated and the next byte is not a terminator,
  ## `AsyncFileLimitError` is raised. The over-limit byte is not consumed —
  ## the file position is left exactly `limit` bytes into the line, so the
  ## caller can recover (e.g. skip to the next terminator) deterministically.
  ##
  ## Seekable files (regular files / block devices) read into a persistent
  ## read-ahead buffer (`rbuf`); bytes past the terminator stay buffered for the
  ## next call, so each byte is pread exactly once. As an implicit-offset op it
  ## shares the single in-flight slot with `read`/`write` (a concurrent one
  ## raises `AsyncFileBusyError`). Any later offset-based op
  ## (`read`/`write`/`writeAt`/`setFileSize`) reconciles by dropping the
  ## read-ahead and rewinding the offset, so positions stay consistent. Non-
  ## seekable fds (pipe/FIFO/tty) still read one byte at a time, leaving bytes
  ## beyond the terminator in the descriptor — preserving streaming semantics
  ## and interleaving with the low-level `readBuffer`.
  ##
  ## After a CR, the byte needed to tell CRLF from a bare CR is *peeked without
  ## blocking* on a non-seekable fd: if it has not arrived yet, the CR is taken
  ## as a bare-CR terminator and the completed line is returned immediately,
  ## rather than suspending until more data or EOF (which would hang a finished
  ## line on an idle stream and deadlock line-based request/response protocols).
  ## The trade-off is that a CRLF whose LF is delivered in a separate, delayed
  ## read (CR and LF split across a stall) is read as a bare CR followed by an
  ## empty line — but only in exactly the situations where the old behaviour
  ## would have hung; when the next byte is already buffered (the common case,
  ## e.g. the writer sent `\c\L` together) CRLF is still recognised as one
  ## terminator.
  ##
  ## If a mid-line read fails (or, on a non-seekable fd, is cancelled), the
  ## partial line accumulated so far is lost with the exception and the file
  ## position remains where reading stopped — i.e. in the middle of the line,
  ## not at its start. The handle itself stays consistent and usable. An
  ## *already complete* line is never lost this way: once its terminator has
  ## been read, a failure of the post-CR peek (the read-ahead refill needed only
  ## to tell CRLF from a bare CR) still returns the line, deferring the error to
  ## the next call.
  checkOpen(f)

  var line = ""
  if not f.seekable:
    # Byte-at-a-time on streams: never read past the newline.
    var any = false
    while true:
      let c = await read(f, 1)
      if c.len == 0:
        break
      any = true
      let ch = char(c[0])
      if ch == '\L':
        break
      if ch == '\c':
        # Distinguish CRLF from a bare CR by peeking the next byte — but never
        # block to do so. A blocking `await read` here would hang an already
        # complete CR-terminated line on an idle stream until more data or EOF
        # (see `tryReadByte`). `got = false` means EOF or the next byte has not
        # arrived yet; either way the line is complete and the CR is a bare CR.
        try:
          let nxt = tryReadByte(f)
          if nxt.got and char(nxt.b) != '\L':
            # Bare CR (not CRLF): un-read the following byte. Non-seekable, so
            # fall back to a logical pushback that `read` re-serves.
            f.pushback.add(nxt.b)
        except AsyncFileError:
          # The peek needed to disambiguate CRLF from a bare CR failed with a
          # real I/O error. The line is already complete (the CR terminated it),
          # so return it now rather than losing it to the exception. The error
          # resurfaces on the next call: tryReadByte left f.offset and pushback
          # unchanged on failure.
          return Opt.some(line)
        break
      if limit > 0 and line.len >= limit:
        # Un-read the over-limit byte so the position stays at the limit
        # boundary (same pushback mechanism as the bare-CR case).
        f.pushback.add(c[0])
        raise newAsyncFileLimitError("line exceeds limit of " & $limit & " bytes")
      line.add(ch)
    if any:
      return Opt.some(line)
    else:
      return Opt.none(string)

  # Seekable: scan a persistent read-ahead buffer (`rbuf`), refilling via pread.
  # Bytes past the terminator stay buffered for the next call, so each byte is
  # pread exactly once (no re-reading the tail). `refillReadBuf` is only called
  # once the buffer is exhausted, so it never drops unconsumed bytes.
  const chunkSize = 4096
  # Hold the offset slot for the whole line (readLine is a multi-refill op): each
  # refill passes `alreadyGuarded = true` so the slot stays held with no gap
  # between refills. `withOffsetGuard` releases it on every exit.
  withOffsetGuard(f):
    var any = false
    while true:
      if f.rpos >= f.rbuf.len:
        if not await refillReadBuf(f, chunkSize, alreadyGuarded = true):
          break # EOF: return whatever `line` accumulated
      let ch = char(f.rbuf[f.rpos])
      inc f.rpos
      any = true
      if ch == '\L':
        return Opt.some(line)
      elif ch == '\c':
        # Need the byte after CR to tell CRLF from a bare CR; refill if the CR
        # was the last buffered byte (it is already consumed, so this is safe).
        if f.rpos >= f.rbuf.len:
          try:
            discard await refillReadBuf(f, chunkSize, alreadyGuarded = true)
          except AsyncFileError:
            # The peek's pread failed with a real I/O error (not EOF). The line
            # is already complete — the CR terminated it — so do not lose it to
            # the exception. Return it now; the error resurfaces on the next
            # call, which re-attempts the refill from the post-CR position
            # (refill left rbuf empty and the offset unchanged, so no byte is
            # skipped or re-read). Mirrors the bare-CR-at-EOF case: an
            # un-peekable next byte still leaves a complete line.
            return Opt.some(line)
        if f.rpos < f.rbuf.len and char(f.rbuf[f.rpos]) == '\L':
          inc f.rpos # CRLF: consume the LF
        # else bare CR (or EOF): leave the peeked byte unconsumed for next call.
        return Opt.some(line)
      else:
        if limit > 0 and line.len >= limit:
          # Leave the over-limit byte unconsumed in the read-ahead, so the
          # position stays at the limit boundary.
          dec f.rpos
          raise newAsyncFileLimitError("line exceeds limit of " & $limit & " bytes")
        line.add(ch)
    if any:
      return Opt.some(line)
    else:
      return Opt.none(string)

template lines*(f: AsyncFile, lineVar: untyped, body: untyped) =
  ## Runs `body` once per remaining line of `f`, binding each line (without its
  ## terminator) to `lineVar`. Stops at end of file. Built on `readLine`, so an
  ## empty line runs `body` with `""` instead of ending the loop, and `break`/
  ## `continue` inside `body` control the iteration as in a plain loop. Must be
  ## used inside an async proc — each line is awaited.
  ##
  ## ```nim
  ## withAsyncFile(f, "/tmp/data.txt", fmRead):
  ##   f.lines(line):
  ##     echo line
  ## ```
  while true:
    let opt = await readLine(f)
    if opt.isNone():
      break
    let lineVar = opt.get()
    body

proc write*(
    f: AsyncFile, data: seq[byte]
): Future[void] {.async: (raises: [AsyncFileError, CancelledError]).} =
  ## Writes all bytes of `data`. `data` is kept alive across the await by the
  ## async environment, so no extra copy is needed. Not atomic on non-seekable
  ## fds (see `writeBuffer`): a cancelled or failed write may leave part of
  ## `data` already written.
  ##
  ## **Concurrency (seekable files):** an implicit-offset op sharing the single
  ## in-flight slot with `read`/`readLine`; a concurrent one raises
  ## `AsyncFileBusyError`. Use `writeAt` for concurrent positioned writes.
  checkOpen(f)
  if data.len == 0:
    return
  await writeBuffer(f, unsafeAddr data[0], data.len)

proc write*(
    f: AsyncFile, data: string
): Future[void] {.async: (raises: [AsyncFileError, CancelledError]).} =
  ## Writes all bytes of the string `data`.
  checkOpen(f)
  if data.len == 0:
    return
  await writeBuffer(f, unsafeAddr data[0], data.len)

proc readBufferAt*(
    f: AsyncFile, offset: int64, buf: pointer, size: int
): Future[int] {.async: (raw: true, raises: [AsyncFileError]).} =
  ## Reads up to `size` bytes into `buf` starting at absolute `offset`, without
  ## using or modifying the file position. Seekable files only (pipes/FIFOs fail
  ## with ESPIPE).
  ##
  ## Argument order: `offset` comes first, consistent with the high-level
  ## `readAt(f, offset, size)` (this is *not* the `pread(fd, buf, size, offset)`
  ## C order — `offset` leads in every `*At` proc).
  ##
  ## Because it never touches `f.offset`, it does not interfere with concurrent
  ## implicit-offset I/O (`read`/`write`) on the same file. Note that on a
  ## seekable fd every operation completes synchronously without yielding to the
  ## dispatcher, so distinct operations never actually overlap mid-flight — the
  ## real value here is offset-independence, not interleaving.
  let uerr = usabilityError(f)

  # Fast path: a usable, non-empty positioned read is exactly the seam — a single
  # seekable read at an explicit offset — so return its future directly (it never
  # touches `f.offset`). No `retFuture` is allocated on this hot path; only the
  # error/empty cases below need one.
  if uerr.isNil and size > 0:
    return readSeekable(f, buf, size, offset, "readAt")

  let retFuture = newFuture[int]("chronos_file.readBufferAt")
  if not uerr.isNil:
    retFuture.fail(uerr)
    return retFuture
  if size < 0:
    # See readBuffer: never let a negative size reach the syscall.
    retFuture.fail(newAsyncFileError("negative read size: " & $size))
    return retFuture
  # size == 0: a no-op positioned read.
  retFuture.complete(0)
  return retFuture

proc writeBufferAt*(
    f: AsyncFile, offset: int64, buf: pointer, size: int
): Future[void] {.async: (raises: [AsyncFileError, CancelledError]).} =
  ## Writes exactly `size` bytes from `buf` at absolute `offset`, without using
  ## or modifying the file position. Seekable files only. Not permitted on files
  ## opened with `fmAppend`: on Linux the kernel ignores `pwrite`'s offset under
  ## `O_APPEND` and appends instead, which would silently violate the
  ## positioned-write contract (and allowing it only on POSIX-conforming
  ## platforms would make behavior platform-dependent).
  ##
  ## Argument order: `offset` comes first, consistent with the high-level
  ## `writeAt(f, offset, data)` (this is *not* the `pwrite(fd, buf, size,
  ## offset)` C order — `offset` leads in every `*At` proc).
  ##
  ## **Concurrency (seekable files):** unlike the positioned *read* family
  ## (offset-independent, never rejected), a positioned write must drop the
  ## `readLine` read-ahead (touching `offset`), so it raises `AsyncFileBusyError`
  ## while a seekable implicit-offset op is in flight. Otherwise it never blocks
  ## concurrent positioned reads.
  let uerr = usabilityError(f)
  if not uerr.isNil:
    raise uerr
  if size < 0:
    # See readBuffer: never let a negative size reach the syscall.
    raise newAsyncFileError("negative write size: " & $size)
  if size == 0:
    # A zero-byte positioned write touches nothing, so treat it as a no-op even
    # in append mode (consistent with the high-level writeAt, which returns
    # early on empty data before this proc is reached).
    return
  if f.appendMode:
    raise newAsyncFileError("positioned write is not allowed in append mode (fmAppend)")

  # A positioned write may overwrite bytes in readLine's read-ahead; drop it so a
  # later implicit read does not serve stale bytes. That touches `offset`, so it
  # is turned away while an implicit-offset op holds the slot (else the reconcile
  # would corrupt that op once the seam yields). The writes below use only the
  # explicit `offset`, so no slot is held across the await.
  checkOffsetIdle(f)
  reconcile(f)
  # Write the whole buffer through the seam one chunk at a time (handling partial
  # writes), advancing the explicit offset only — `f.offset` is never touched.
  # Append mode was rejected above, so the seam takes its `pwrite` path here.
  var written = 0
  while written < size:
    let p = cast[pointer](cast[uint](buf) + uint(written))
    written.inc(
      await writeSeekable(f, p, size - written, offset + int64(written), "writeAt")
    )

proc readAt*(
    f: AsyncFile, offset: int64, size: int
): Future[seq[byte]] {.async: (raises: [AsyncFileError, CancelledError]).} =
  ## Reads up to `size` bytes at absolute `offset` and returns them. An empty
  ## seq means end of file. Does not touch the file position.
  checkOpen(f) # even for size <= 0, so a closed handle is always rejected

  if size <= 0:
    return newSeq[byte](0)
  var buffer = newSeq[byte](size)
  let n = await readBufferAt(f, offset, addr buffer[0], size)
  buffer.setLen(n)
  return buffer

proc writeAt*(
    f: AsyncFile, offset: int64, data: seq[byte]
): Future[void] {.async: (raises: [AsyncFileError, CancelledError]).} =
  ## Writes all bytes of `data` at absolute `offset`. Does not touch the file
  ## position.
  checkOpen(f) # even for empty data, so a closed handle is always rejected

  if data.len == 0:
    return
  await writeBufferAt(f, offset, unsafeAddr data[0], data.len)

proc writeAt*(
    f: AsyncFile, offset: int64, data: string
): Future[void] {.async: (raises: [AsyncFileError, CancelledError]).} =
  ## Writes all bytes of the string `data` at absolute `offset`.
  checkOpen(f) # even for empty data, so a closed handle is always rejected

  if data.len == 0:
    return
  await writeBufferAt(f, offset, unsafeAddr data[0], data.len)
