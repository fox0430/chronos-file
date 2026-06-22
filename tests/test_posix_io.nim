## Tests for `chronos_file/posix_io`: the read/write surface — `read`/`write`,
## `readBuffer`/`writeBuffer`, `readExactly`, `readLine`, `readAll`/`readAllString`,
## the positioned `readAt`/`writeAt` family, plus the FIFO async (`addReader2`/
## `addWriter2`), pushback and read-ahead paths.

from std/posix import mkfifo, Mode

import pkg/chronos/unittest2/asynctests

import ../chronos_file
import helpers

suite "chronos_file: posix_io (read/write surface)":
  teardown:
    checkLeaks()

  asyncTest "write then readAll roundtrip (fmReadWrite)":
    let path = tempPath(".txt")
    let f = openAsync(path, fmReadWrite)
    await f.write(@[byte 1, 2, 3, 4, 5])
    f.setFilePos(0)
    let data = await f.readAll()
    check data == @[byte 1, 2, 3, 4, 5]
    f.close()
    removeFile(path)

  asyncTest "readBuffer / writeBuffer (pointer API)":
    let path = tempPath(".bin")
    let f = openAsync(path, fmReadWrite)
    var src = @[byte 10, 20, 30, 40]
    await f.writeBuffer(addr src[0], src.len)
    f.setFilePos(0)
    var dst = newSeq[byte](src.len)
    let n = await f.readBuffer(addr dst[0], dst.len)
    check n == src.len
    check dst == src
    f.close()
    removeFile(path)

  asyncTest "read(size) partial reads and EOF":
    let path = tempPath(".bin")
    let f = openAsync(path, fmReadWrite)
    await f.write(@[byte 1, 2, 3])
    f.setFilePos(0)
    check (await f.read(2)) == @[byte 1, 2]
    check (await f.read(2)) == @[byte 3]
    check (await f.read(2)).len == 0
    f.close()
    removeFile(path)

  asyncTest "readExactly fills the buffer and raises on early EOF (#6)":
    let path = tempPath(".bin")
    let f = openAsync(path, fmReadWrite)
    await f.write(@[byte 1, 2, 3, 4, 5])
    f.setFilePos(0)
    check (await f.readExactly(3)) == @[byte 1, 2, 3]
    check (await f.readExactly(2)) == @[byte 4, 5]
    # At EOF, asking for more raises AsyncFileIncompleteError.
    var failed = false
    try:
      discard await f.readExactly(1)
    except AsyncFileIncompleteError:
      failed = true
    check failed
    # A request larger than what remains also raises.
    f.setFilePos(0)
    var failed2 = false
    try:
      discard await f.readExactly(10)
    except AsyncFileIncompleteError:
      failed2 = true
    check failed2
    # size <= 0 is an empty no-op.
    check (await f.readExactly(0)).len == 0
    f.close()
    removeFile(path)

  asyncTest "readExactly serves readLine pushback first on a FIFO":
    # On a non-seekable fd a bare CR makes readLine push the next byte back.
    # readExactly must serve that pushed-back byte before issuing a syscall: it
    # goes through the same pushback-aware path as read(), not the low-level
    # readBuffer (which bypasses pushback). Guards the readInto refactor.
    let path = tempPath(".fifo")
    check mkfifo(cstring(path), Mode(0o600)) == 0
    let r = openAsync(path, fmRead)
    let w = openAsync(path, fmWrite)
    await w.write("a\rXYZ")
    w.close() # reader drains the buffered bytes, then sees EOF
    check (await r.readLine()) == Opt.some("a") # bare CR -> 'X' left in pushback
    check (await r.readExactly(3)) == @[byte 'X', byte 'Y', byte 'Z']
    r.close()
    removeFile(path)

  asyncTest "readExactly fills across short reads on a FIFO":
    # readExactly must loop over short reads. With only 3 of the 6 requested
    # bytes buffered, the first readInto returns 3 and the loop suspends for the
    # rest; a second write then completes it.
    let path = tempPath(".fifo")
    check mkfifo(cstring(path), Mode(0o600)) == 0
    let r = openAsync(path, fmRead)
    let w = openAsync(path, fmWrite)
    await w.write(@[byte 1, 2, 3]) # only 3 of the 6 requested are available
    let rfut = r.readExactly(6)
    # First readInto consumed the 3 buffered bytes; the loop now waits for more.
    check not rfut.finished()
    await w.write(@[byte 4, 5, 6])
    check (await rfut) == @[byte 1, 2, 3, 4, 5, 6]
    r.close()
    w.close()
    removeFile(path)

  asyncTest "readLine handles \\n, \\c\\L and final line":
    let path = tempPath(".txt")
    let f = openAsync(path, fmReadWrite)
    await f.write("line1\nline2\r\nlast")
    f.setFilePos(0)
    check (await f.readLine()) == Opt.some("line1")
    check (await f.readLine()) == Opt.some("line2")
    check (await f.readLine()) == Opt.some("last")
    check (await f.readLine()).isNone()
    f.close()
    removeFile(path)

  asyncTest "readLine handles a bare CR ending exactly at a read-ahead chunk boundary":
    # The read-ahead chunk is 4096 bytes. A bare CR as the very last byte of a
    # chunk forces readLine to refill (pread the next chunk) purely to peek the
    # byte after the CR. Here the next chunk is EOF, so the line is complete and
    # must be returned — exercising the post-CR refill branch that now also
    # guards against losing a completed line if that peek pread fails.
    let path = tempPath(".txt")
    let f = openAsync(path, fmReadWrite)
    let body = "a".repeat(4095) # 4095 + the CR = exactly one 4096-byte chunk
    await f.write(body & "\r")
    f.setFilePos(0)
    check (await f.readLine()) == Opt.some(body) # bare CR at the chunk/EOF edge
    check (await f.readLine()).isNone()
    check f.getFilePos() == 4096 # positioned right after the consumed CR
    f.close()
    removeFile(path)

  asyncTest "readLine recognises a CRLF split across a read-ahead chunk boundary":
    # CR is the last byte of chunk 1; its LF is the first byte of chunk 2. The
    # post-CR refill must fetch chunk 2 and recognise the CRLF as one terminator,
    # not emit a spurious empty line for the LF.
    let path = tempPath(".txt")
    let f = openAsync(path, fmReadWrite)
    let body = "a".repeat(4095) # CR lands at byte 4095 (last of chunk 1)
    await f.write(body & "\r\ntail") # LF is byte 4096 (first of chunk 2)
    f.setFilePos(0)
    check (await f.readLine()) == Opt.some(body) # CRLF spanning the boundary
    check (await f.readLine()) == Opt.some("tail") # next line, no spurious "" first
    check (await f.readLine()).isNone()
    f.close()
    removeFile(path)

  asyncTest "large file across multiple chunks (>4KB)":
    let path = tempPath(".bin")
    let f = openAsync(path, fmReadWrite)
    var big = newSeq[byte](10000)
    for i in 0 ..< big.len:
      big[i] = byte(i mod 256)
    await f.write(big)
    f.setFilePos(0)
    let rd = await f.readAll()
    check rd == big
    f.close()
    removeFile(path)

  asyncTest "fmAppend write after setFilePos still appends (sequential write)":
    # Append-mode writes go through a sequential write() so the kernel itself
    # picks the end of file on every platform. With the previous pwrite-based
    # path this only held on Linux (its O_APPEND quirk): on macOS/BSD the
    # second write below would have overwritten at offset 0.
    let path = tempPath(".txt")
    await writeFileAsync(path, "abc")
    let f = openAsync(path, fmAppend)
    await f.write("def")
    f.setFilePos(0)
    await f.write("ghi")
    f.close()
    check (await readFileAsync(path)) == "abcdefghi"
    removeFile(path)

  asyncTest "write(string) and read back":
    let path = tempPath(".txt")
    let f = openAsync(path, fmReadWrite)
    await f.write("hello")
    f.setFilePos(0)
    check (await f.readAll()) == toBytes("hello")
    f.close()
    removeFile(path)

  asyncTest "FIFO read takes async EAGAIN -> addReader2 path":
    let path = tempPath(".fifo")
    check mkfifo(cstring(path), Mode(0o600)) == 0
    let r = openAsync(path, fmRead)
    let w = openAsync(path, fmWrite)
    let rfut = r.read(5)
    check not rfut.finished()
    await w.write(@[byte 1, 2, 3, 4, 5])
    let data = await rfut
    check data == @[byte 1, 2, 3, 4, 5]
    r.close()
    w.close()
    removeFile(path)

  asyncTest "cancelling a pending FIFO read removes the reader":
    let path = tempPath(".fifo")
    check mkfifo(cstring(path), Mode(0o600)) == 0
    let r = openAsync(path, fmRead)
    let w = openAsync(path, fmWrite)
    let rfut = r.read(5)
    check not rfut.finished()
    await rfut.cancelAndWait()
    check rfut.cancelled()
    r.close()
    w.close()
    removeFile(path)

  asyncTest "readLine preserves the byte after a bare CR":
    let path = tempPath(".txt")
    let f = openAsync(path, fmReadWrite)
    await f.write("a\rb")
    f.setFilePos(0)
    check (await f.readLine()) == Opt.some("a")
    check (await f.readLine()) == Opt.some("b")
    check (await f.readLine()).isNone()
    f.close()
    removeFile(path)

  asyncTest "FIFO write larger than the pipe buffer drains via addWriter2":
    let path = tempPath(".fifo")
    check mkfifo(cstring(path), Mode(0o600)) == 0
    let r = openAsync(path, fmRead)
    let w = openAsync(path, fmWrite)
    var big = newSeq[byte](256 * 1024)
    for i in 0 ..< big.len:
      big[i] = byte(i mod 251)
    let wfut = w.write(big)
    check not wfut.finished()
    var got: seq[byte] = @[]
    while got.len < big.len:
      let chunk = await r.read(big.len - got.len)
      if chunk.len == 0:
        break
      got.add(chunk)
    await wfut
    check got == big
    r.close()
    w.close()
    removeFile(path)

  asyncTest "cancelling a pending FIFO write removes the writer":
    let path = tempPath(".fifo")
    check mkfifo(cstring(path), Mode(0o600)) == 0
    let r = openAsync(path, fmRead)
    let w = openAsync(path, fmWrite)
    var big = newSeq[byte](1024 * 1024)
    let wfut = w.write(big)
    check not wfut.finished()
    await wfut.cancelAndWait()
    check wfut.cancelled()
    r.close()
    w.close()
    removeFile(path)

  asyncTest "readLine handles a bare CR on a non-seekable FIFO":
    let path = tempPath(".fifo")
    check mkfifo(cstring(path), Mode(0o600)) == 0
    let r = openAsync(path, fmRead)
    let w = openAsync(path, fmWrite)
    await w.write("a\rb")
    w.close() # so the reader sees EOF after the pushed-back byte
    check (await r.readLine()) == Opt.some("a")
    check (await r.readLine()) == Opt.some("b")
    check (await r.readLine()).isNone()
    r.close()
    removeFile(path)

  asyncTest "readLine returns a bare-CR line on an idle stream without blocking":
    # Regression: a CR-terminated line on a non-seekable fd must not block waiting
    # for the byte after the CR (needed to tell CRLF from a bare CR). The writer
    # sends "hello\r" and then pauses — neither appends nor closes — as a
    # line-based request/response peer would while awaiting our reply. readLine
    # must return the already-complete "hello" now (peeking the next byte
    # non-blockingly), not suspend on addReader2 until more data or EOF.
    let path = tempPath(".fifo")
    check mkfifo(cstring(path), Mode(0o600)) == 0
    let r = openAsync(path, fmRead)
    let w = openAsync(path, fmWrite)
    await w.write("hello\r") # no LF, no close: the stream is now idle
    let lf = r.readLine()
    check lf.finished() # completed synchronously — no suspension on the CR peek
    check (await lf) == Opt.some("hello")
    # The stream is still open, and a CRLF delivered together is still one line.
    await w.write("world\r\n")
    check (await r.readLine()) == Opt.some("world")
    w.close()
    check (await r.readLine()).isNone()
    r.close()
    removeFile(path)

  asyncTest "readLine returns a completed bare-CR line when the non-seekable peek fails":
    # Construct a non-seekable handle whose only available byte is a CR in the
    # readLine pushback. The post-CR peek then hits a broken fd (EBADF). The
    # completed line must be returned, and the error must resurface on the next
    # readLine.
    var f = AsyncFile(
      fd: AsyncFD(cint(-1)),
      offset: 0,
      seekable: false,
      opened: true,
      pushback: @[byte '\r'],
    )
    try:
      check (await f.readLine()) == Opt.some("")
      var failed = false
      try:
        discard await f.readLine()
      except AsyncFileOsError:
        failed = true
      check failed
    finally:
      # Always clear `opened` before the handle is destroyed: were a regression
      # to make the first readLine raise instead, an unwinding destructor would
      # unregister2/closeFd the invalid fd (and unregistering an fd that was
      # never registered aborts in the dispatcher), masking the test failure.
      f.opened = false

  asyncTest "readLine returns a completed bare-CR line when the seekable peek pread fails":
    # Construct a seekable handle with a full chunk whose last byte is a CR.
    # The fd is invalid, so the post-CR refill (the peek needed to tell CRLF
    # from a bare CR) fails with EBADF. The completed line must still be
    # returned, and the error must resurface on the next call.
    let body = "a".repeat(4095)
    var f = AsyncFile(
      fd: AsyncFD(cint(-1)),
      offset: 4096,
      seekable: true,
      opened: true,
      rbuf: (body & "\r").toBytes,
      rpos: 0,
    )
    try:
      check (await f.readLine()) == Opt.some(body)
      var failed = false
      try:
        discard await f.readLine()
      except AsyncFileOsError:
        failed = true
      check failed
    finally:
      # Always clear `opened` before the handle is destroyed (see the
      # non-seekable peek-failure test): a regression that raised on the first
      # readLine would otherwise let the destructor closeFd the invalid fd while
      # unwinding, masking the failure.
      f.opened = false

  asyncTest "readAt / writeAt do not disturb the file position":
    let path = tempPath(".bin")
    let f = openAsync(path, fmReadWrite)
    await f.write(@[byte 0, 1, 2, 3, 4, 5, 6, 7, 8, 9])
    f.setFilePos(7)
    check (await f.readAt(2, 3)) == @[byte 2, 3, 4]
    check f.getFilePos() == 7
    await f.writeAt(0, @[byte 99])
    check f.getFilePos() == 7
    check (await f.readAt(0, 1)) == @[byte 99]
    # The implicit position still works and resumes from where it was.
    check (await f.read(1)) == @[byte 7]
    check f.getFilePos() == 8
    f.close()
    removeFile(path)

  asyncTest "writeAt(string) and positioned overwrite":
    let path = tempPath(".txt")
    let f = openAsync(path, fmReadWrite)
    await f.write("hello world")
    await f.writeAt(6, "moon!")
    f.setFilePos(0)
    check (await f.readAll()) == toBytes("hello moon!")
    f.close()
    removeFile(path)

  asyncTest "concurrent readAt calls do not interfere":
    let path = tempPath(".bin")
    let f = openAsync(path, fmReadWrite)
    await f.write(@[byte 0, 1, 2, 3, 4, 5, 6, 7])
    let fa = f.readAt(0, 4)
    let fb = f.readAt(4, 4)
    await allFutures(fa, fb)
    check (await fa) == @[byte 0, 1, 2, 3]
    check (await fb) == @[byte 4, 5, 6, 7]
    f.close()
    removeFile(path)

  asyncTest "readBufferAt/writeBufferAt put offset first (#5)":
    let path = tempPath(".bin")
    let f = openAsync(path, fmReadWrite)
    await f.write(@[byte 0, 0, 0, 0, 0, 0, 0, 0])
    var src = @[byte 7, 8, 9]
    # offset is the first argument (consistent with readAt/writeAt).
    await f.writeBufferAt(int64(2), addr src[0], src.len)
    var dst = newSeq[byte](3)
    let n = await f.readBufferAt(int64(2), addr dst[0], dst.len)
    check n == 3
    check dst == src
    f.close()
    removeFile(path)

  asyncTest "readAt fails on a non-seekable FIFO":
    let path = tempPath(".fifo")
    check mkfifo(cstring(path), Mode(0o600)) == 0
    let r = openAsync(path, fmRead)
    let w = openAsync(path, fmWrite)
    var failed = false
    try:
      discard await r.readAt(0, 4)
    except AsyncFileError:
      failed = true
    check failed
    r.close()
    w.close()
    removeFile(path)

  asyncTest "writing to a file opened fmRead fails":
    let path = tempPath(".bin")
    block:
      let f = openAsync(path, fmWrite)
      await f.write(@[byte 1])
      f.close()
    let f = openAsync(path, fmRead)
    var failed = false
    try:
      await f.write(@[byte 2])
    except AsyncFileError:
      failed = true
    check failed
    f.close()
    removeFile(path)

  asyncTest "a second concurrent read is rejected (#2)":
    let path = tempPath(".fifo")
    check mkfifo(cstring(path), Mode(0o600)) == 0
    let r = openAsync(path, fmRead)
    let w = openAsync(path, fmWrite)
    let rfut1 = r.read(5)
    check not rfut1.finished()
    expect(AsyncFileBusyError):
      discard await r.read(5)
    await w.write(@[byte 1, 2, 3, 4, 5])
    check (await rfut1) == @[byte 1, 2, 3, 4, 5]
    r.close()
    w.close()
    removeFile(path)

  asyncTest "writeAt on an fmAppend file is rejected (#4)":
    let path = tempPath(".bin")
    block:
      let f = openAsync(path, fmWrite)
      await f.write(@[byte 1, 2, 3])
      f.close()
    let f = openAsync(path, fmAppend)
    var failed = false
    try:
      await f.writeAt(0, @[byte 9])
    except AsyncFileError:
      failed = true
    check failed
    f.close()
    removeFile(path)

  asyncTest "low-level writeBuffer/readBuffer on a FIFO (async path)":
    let path = tempPath(".fifo")
    check mkfifo(cstring(path), Mode(0o600)) == 0
    let r = openAsync(path, fmRead)
    let w = openAsync(path, fmWrite)
    var dst = newSeq[byte](4)
    let rfut = r.readBuffer(addr dst[0], dst.len)
    check not rfut.finished()
    var src = @[byte 10, 20, 30, 40]
    await w.writeBuffer(addr src[0], src.len)
    let n = await rfut
    check n == 4
    check dst == src
    r.close()
    w.close()
    removeFile(path)

  asyncTest "zero-size reads and writes are no-ops":
    let path = tempPath(".bin")
    let f = openAsync(path, fmReadWrite)
    await f.write(@[byte 1, 2, 3])
    # Zero-length writes must not advance the position or touch the file.
    await f.write(newSeq[byte](0))
    await f.writeBuffer(nil, 0)
    f.setFilePos(0)
    # Zero-length reads return empty / 0 and consume nothing.
    check (await f.read(0)).len == 0
    var dummy: byte
    check (await f.readBuffer(addr dummy, 0)) == 0
    check f.getFilePos() == 0
    check (await f.read(3)) == @[byte 1, 2, 3]
    f.close()
    removeFile(path)

  asyncTest "cancelling a pending FIFO write(string) removes the writer":
    # write(string) is a separate overload from write(seq[byte]); make sure its
    # cancellation path behaves the same as the seq[byte] one tested above.
    let path = tempPath(".fifo")
    check mkfifo(cstring(path), Mode(0o600)) == 0
    let r = openAsync(path, fmRead)
    let w = openAsync(path, fmWrite)
    let big = newString(1024 * 1024)
    let wfut = w.write(big)
    check not wfut.finished()
    await wfut.cancelAndWait()
    check wfut.cancelled()
    r.close()
    w.close()
    removeFile(path)

  asyncTest "cancelling a pending FIFO readAll removes the reader":
    # readAll loops over read(); with nothing buffered it suspends on the first
    # chunk. Cancelling must tear the reader down cleanly, so the later close()
    # has no in-flight read to fail (mirrors the single-read cancel test above).
    let path = tempPath(".fifo")
    check mkfifo(cstring(path), Mode(0o600)) == 0
    let r = openAsync(path, fmRead)
    let w = openAsync(path, fmWrite)
    let rfut = r.readAll()
    check not rfut.finished()
    await rfut.cancelAndWait()
    check rfut.cancelled()
    r.close()
    w.close()
    removeFile(path)

  asyncTest "readBuffer bypasses readLine pushback on a FIFO":
    # On a non-seekable fd a bare CR makes readLine push the following byte back
    # (it cannot lseek to un-read it). The low-level readBuffer must ignore that
    # pushback and read straight from the descriptor.
    let path = tempPath(".fifo")
    check mkfifo(cstring(path), Mode(0o600)) == 0
    let r = openAsync(path, fmRead)
    let w = openAsync(path, fmWrite)
    await w.write("a\rXY")
    w.close() # reader still drains the buffered bytes, then sees EOF
    check (await r.readLine()) == Opt.some("a") # bare CR -> 'X' left in pushback
    var dst = newSeq[byte](1)
    let n = await r.readBuffer(addr dst[0], dst.len)
    check n == 1
    check dst == @[byte 'Y'] # 'X' stayed in pushback; the syscall returns 'Y'
    r.close()
    removeFile(path)

  asyncTest "read with pushback returns a short read instead of suspending":
    # A bare CR leaves a byte in readLine's pushback. A read() holding that
    # deliverable byte must not suspend: on EAGAIN it returns a short read.
    let path = tempPath(".fifo")
    check mkfifo(cstring(path), Mode(0o600)) == 0
    let r = openAsync(path, fmRead)
    let w = openAsync(path, fmWrite)
    await w.write("a\rX") # readLine pushes 'X' back
    check (await r.readLine()) == Opt.some("a")
    # The descriptor is empty, so read(5) completes immediately with just 'X'.
    let rfut = r.read(5)
    check rfut.finished()
    check (await rfut) == @[byte 'X']
    await w.write("YZWV")
    w.close()
    check (await r.readAll()) == toBytes("YZWV")
    r.close()
    removeFile(path)

  asyncTest "read combines pushback with buffered bytes in one call":
    # With data buffered in the descriptor, the non-blocking top-up must
    # deliver pushback + fresh bytes together.
    let path = tempPath(".fifo")
    check mkfifo(cstring(path), Mode(0o600)) == 0
    let r = openAsync(path, fmRead)
    let w = openAsync(path, fmWrite)
    await w.write("a\rXYZ") # readLine pushes 'X' back; "YZ" stays buffered
    check (await r.readLine()) == Opt.some("a")
    let rfut = r.read(5)
    check rfut.finished()
    check (await rfut) == toBytes("XYZ")
    w.close()
    r.close()
    removeFile(path)

  asyncTest "a second concurrent write is rejected with AsyncFileBusyError":
    # Mirror of the read-rejection test (#2) for the write slot.
    let path = tempPath(".fifo")
    check mkfifo(cstring(path), Mode(0o600)) == 0
    let r = openAsync(path, fmRead)
    let w = openAsync(path, fmWrite)
    # Exceed the pipe buffer so the first write suspends on addWriter2.
    let big = newSeq[byte](2 * 1024 * 1024)
    let wfut1 = w.write(big)
    check not wfut1.finished()
    expect(AsyncFileBusyError):
      await w.write(@[byte 1, 2, 3])
    await wfut1.cancelAndWait()
    r.close()
    w.close()
    removeFile(path)

  asyncTest "writing to a FIFO whose reader closed fails with EPIPE (no crash)":
    # The last reader closing makes writes return EPIPE. chronos ignores SIGPIPE
    # at dispatcher init, so this must surface as an exception, not kill the
    # process.
    let path = tempPath(".fifo")
    check mkfifo(cstring(path), Mode(0o600)) == 0
    let r = openAsync(path, fmRead)
    let w = openAsync(path, fmWrite)
    r.close()
    var failed = false
    try:
      await w.write(@[byte 1, 2, 3])
    except AsyncFileError:
      failed = true
    check failed
    w.close()
    removeFile(path)

  asyncTest "writeAt past EOF leaves a zero-filled hole":
    let path = tempPath(".bin")
    let f = openAsync(path, fmReadWrite)
    await f.write(@[byte 1, 2, 3])
    await f.writeAt(10, @[byte 9])
    check f.getFileSize() == 11
    f.setFilePos(0)
    check (await f.readAll()) == @[byte 1, 2, 3, 0, 0, 0, 0, 0, 0, 0, 9]
    f.close()
    removeFile(path)

  asyncTest "readLine reads a line longer than one chunk (M3)":
    let path = tempPath(".txt")
    let f = openAsync(path, fmReadWrite)
    let longLine = repeat('a', 5000) # > 4096 chunk
    await f.write(longLine & "\nrest")
    f.setFilePos(0)
    check (await f.readLine()) == Opt.some(longLine)
    check (await f.readLine()) == Opt.some("rest")
    check (await f.readLine()).isNone()
    f.close()
    removeFile(path)

  asyncTest "readLine handles a CRLF straddling a chunk boundary (M3)":
    let path = tempPath(".txt")
    let f = openAsync(path, fmReadWrite)
    # Put CR at index 4095 (last byte of chunk 1) and LF at 4096 (start of 2).
    let prefix = repeat('a', 4095)
    await f.write(prefix & "\r\nrest")
    f.setFilePos(0)
    check (await f.readLine()) == Opt.some(prefix)
    check (await f.readLine()) == Opt.some("rest")
    check (await f.readLine()).isNone()
    f.close()
    removeFile(path)

  asyncTest "readLine handles a bare CR at a chunk boundary (M3)":
    let path = tempPath(".txt")
    let f = openAsync(path, fmReadWrite)
    # CR at index 4095, next byte is not LF -> bare CR ends the line, byte kept.
    let prefix = repeat('a', 4095)
    await f.write(prefix & "\rb")
    f.setFilePos(0)
    check (await f.readLine()) == Opt.some(prefix)
    check (await f.readLine()) == Opt.some("b")
    check (await f.readLine()).isNone()
    f.close()
    removeFile(path)

  asyncTest "readLine leaves position right after the terminator (M3)":
    let path = tempPath(".txt")
    let f = openAsync(path, fmReadWrite)
    await f.write("ab\ncd")
    f.setFilePos(0)
    check (await f.readLine()) == Opt.some("ab")
    # After the '\n' the position must be at 3, so a raw read sees "cd".
    check f.getFilePos() == 3
    check (await f.read(2)) == toBytes("cd")
    f.close()
    removeFile(path)

  asyncTest "readLine read-ahead is invalidated by an overlapping writeAt (#2)":
    # readLine buffers the post-terminator tail; a positioned write into that
    # region must drop the read-ahead so the next read sees the new bytes.
    let path = tempPath(".txt")
    let f = openAsync(path, fmReadWrite)
    await f.write("line1\nline2\nline3")
    f.setFilePos(0)
    check (await f.readLine()) == Opt.some("line1") # buffers "line2\nline3"
    await f.writeAt(6, "L") # "line2" -> "Line2", inside the buffered region
    check (await f.readLine()) == Opt.some("Line2") # must be the post-write byte
    check (await f.readLine()) == Opt.some("line3")
    check (await f.readLine()).isNone()
    f.close()
    removeFile(path)

  asyncTest "readLine read-ahead is invalidated by setFileSize (#2)":
    let path = tempPath(".txt")
    let f = openAsync(path, fmReadWrite)
    await f.write("aaa\nbbb\nccc")
    f.setFilePos(0)
    check (await f.readLine()) == Opt.some("aaa") # buffers "bbb\nccc"
    check f.getFilePos() == 4
    f.setFileSize(4) # truncate to "aaa\n"; read-ahead is now stale
    check (await f.readLine()).isNone() # reading from pos 4 sees EOF
    check f.getFilePos() == 4
    f.close()
    removeFile(path)

  asyncTest "writeAt with buffered read-ahead preserves getFilePos (#2)":
    let path = tempPath(".txt")
    let f = openAsync(path, fmReadWrite)
    await f.write("alpha\nbeta\ngamma")
    f.setFilePos(0)
    check (await f.readLine()) == Opt.some("alpha") # buffers the tail
    let pos = f.getFilePos()
    check pos == 6
    await f.writeAt(0, "A") # positioned write drops read-ahead but must not move pos
    check f.getFilePos() == pos
    check (await f.readLine()) == Opt.some("beta") # reads resume from pos
    f.close()
    removeFile(path)

  asyncTest "a failed readLine refill does not corrupt the handle state":
    # pread on a write-only fd fails deterministically. The grown zero-filled
    # rbuf must not survive the failure, or the next readLine would serve
    # phantom zeros and getFilePos would drift.
    let path = tempPath(".bin")
    let f = openAsync(path, fmWrite)
    await f.write(@[byte 'x'])
    var failed = false
    try:
      discard await f.readLine()
    except AsyncFileOsError:
      failed = true
    check failed
    # No phantom read-ahead: the logical position is unchanged.
    check f.getFilePos() == 1
    # A second attempt fails the same way instead of serving zero bytes.
    var failed2 = false
    try:
      discard await f.readLine()
    except AsyncFileOsError:
      failed2 = true
    check failed2
    check f.getFilePos() == 1
    f.close()
    removeFile(path)

  asyncTest "readLine streams many short lines across chunk refills (#2)":
    # 2000 short lines (> one 4096 chunk) exercise refill-on-exhaust plus
    # buffered-tail reuse; content and final position must stay exact.
    let path = tempPath(".txt")
    let f = openAsync(path, fmReadWrite)
    var expected: seq[string] = @[]
    var content = ""
    for i in 0 ..< 2000:
      let s = "line" & $i
      expected.add(s)
      content.add(s & "\n")
    await f.write(content)
    f.setFilePos(0)
    for i in 0 ..< expected.len:
      check (await f.readLine()) == Opt.some(expected[i])
    check (await f.readLine()).isNone()
    check f.getFilePos() == int64(content.len)
    f.close()
    removeFile(path)

  asyncTest "readLine then read then readLine reconciles the read-ahead (T2)":
    # readLine fills the seekable read-ahead buffer; a following raw read must
    # reconcile (drop the read-ahead, rewind offset, re-pread), and a later
    # readLine must resume correctly from the reconciled position.
    let path = tempPath(".txt")
    let f = openAsync(path, fmReadWrite)
    await f.write("alpha\nbeta\ngamma\n")
    f.setFilePos(0)
    check (await f.readLine()) == Opt.some("alpha") # buffers "beta\ngamma\n" in rbuf
    check f.getFilePos() == 6
    # Raw read reconciles and re-preads from the logical position (offset 6).
    check (await f.read(4)) == toBytes("beta")
    check f.getFilePos() == 10
    # readLine resumes after the reconciled read: the '\n' right after "beta"
    # yields an empty line (Opt.some(""), distinct from EOF), then "gamma", then EOF.
    check (await f.readLine()) == Opt.some("")
    check (await f.readLine()) == Opt.some("gamma")
    check (await f.readLine()).isNone()
    f.close()
    removeFile(path)

  asyncTest "raw procs reject a negative size":
    # The high-level read/readExactly/readAt treat size <= 0 as empty, but the
    # raw pointer procs used to pass a negative size through: read wrapped it to
    # a huge csize_t (pread -> EFAULT), write completed silently. All four must
    # fail with AsyncFileError instead of reaching the syscall.
    let path = tempPath(".bin")
    let f = openAsync(path, fmReadWrite)
    await f.write(@[byte 1, 2, 3])
    var dummy: byte
    template rejects(body: untyped) =
      var failed = false
      try:
        body
      except AsyncFileError:
        failed = true
      check failed

    rejects:
      discard await f.readBuffer(addr dummy, -5)
    rejects:
      await f.writeBuffer(addr dummy, -5)
    rejects:
      discard await f.readBufferAt(0, addr dummy, -5)
    rejects:
      await f.writeBufferAt(0, addr dummy, -5)
    # The handle stays usable and the position untouched.
    f.setFilePos(0)
    check (await f.read(3)) == @[byte 1, 2, 3]
    f.close()
    removeFile(path)

  asyncTest "readLine distinguishes empty lines from EOF (Option)":
    let path = tempPath(".txt")
    let f = openAsync(path, fmReadWrite)
    await f.write("a\n\nb")
    f.setFilePos(0)
    check (await f.readLine()) == Opt.some("a")
    check (await f.readLine()) == Opt.some("") # empty line, not EOF
    check (await f.readLine()) == Opt.some("b")
    check (await f.readLine()).isNone() # EOF
    check (await f.readLine()).isNone() # stays none on repeated calls
    f.close()
    removeFile(path)

  asyncTest "readLine distinguishes empty lines from EOF on a FIFO (Option)":
    let path = tempPath(".fifo")
    check mkfifo(cstring(path), Mode(0o600)) == 0
    let r = openAsync(path, fmRead)
    let w = openAsync(path, fmWrite)
    await w.write("a\n\nb")
    w.close() # reader drains the buffered bytes, then sees EOF
    check (await r.readLine()) == Opt.some("a")
    check (await r.readLine()) == Opt.some("")
    check (await r.readLine()) == Opt.some("b")
    check (await r.readLine()).isNone()
    r.close()
    removeFile(path)

  asyncTest "readLine on an empty file returns none immediately":
    let path = tempPath(".txt")
    let f = openAsync(path, fmReadWrite)
    check (await f.readLine()).isNone()
    f.close()
    removeFile(path)

  asyncTest "readLine treats a lone trailing bare CR as an empty line":
    # A file ending in a single "\r" is one empty line terminated by CR — it
    # consumed a byte, so it must be Opt.some(""), and only the next call is EOF.
    let path = tempPath(".txt")
    let f = openAsync(path, fmReadWrite)
    await f.write("\r")
    f.setFilePos(0)
    check (await f.readLine()) == Opt.some("")
    check (await f.readLine()).isNone()
    f.close()
    removeFile(path)

  asyncTest "readLine with a trailing newline yields the line then none":
    let path = tempPath(".txt")
    let f = openAsync(path, fmReadWrite)
    await f.write("x\n")
    f.setFilePos(0)
    check (await f.readLine()) == Opt.some("x")
    check (await f.readLine()).isNone()
    f.close()
    removeFile(path)

  asyncTest "readLine limit raises and leaves the position at the boundary":
    let path = tempPath(".txt")
    let f = openAsync(path, fmReadWrite)
    await f.write("abcdef\nrest")
    f.setFilePos(0)
    var limited = false
    try:
      discard await f.readLine(limit = 3)
    except AsyncFileLimitError:
      limited = true
    check limited
    # The over-limit byte ('d') was not consumed: exactly `limit` bytes in.
    check f.getFilePos() == 3
    # An unlimited readLine resumes from the boundary.
    check (await f.readLine()) == Opt.some("def")
    check (await f.readLine()) == Opt.some("rest")
    check (await f.readLine()).isNone()
    f.close()
    removeFile(path)

  asyncTest "readLine limit on a FIFO pushes the over-limit byte back":
    let path = tempPath(".fifo")
    check mkfifo(cstring(path), Mode(0o600)) == 0
    let r = openAsync(path, fmRead)
    let w = openAsync(path, fmWrite)
    await w.write("abcdef\n")
    var limited = false
    try:
      discard await r.readLine(limit = 3)
    except AsyncFileLimitError:
      limited = true
    check limited
    # 'd' went into the pushback, so the next readLine resumes with it.
    check (await r.readLine()) == Opt.some("def")
    w.close()
    check (await r.readLine()).isNone()
    r.close()
    removeFile(path)

  asyncTest "readLine limit: a line of exactly limit bytes succeeds":
    let path = tempPath(".txt")
    let f = openAsync(path, fmReadWrite)
    await f.write("abc\nxy")
    f.setFilePos(0)
    check (await f.readLine(limit = 3)) == Opt.some("abc")
    # limit = 0 means unlimited (the default).
    check (await f.readLine(limit = 0)) == Opt.some("xy")
    f.close()
    removeFile(path)

  asyncTest "readAllString reads the whole file as a string":
    let path = tempPath(".txt")
    let f = openAsync(path, fmReadWrite)
    await f.write("hello world")
    f.setFilePos(0)
    check (await f.readAllString()) == "hello world"
    # From mid-position, like readAll.
    f.setFilePos(6)
    check (await f.readAllString()) == "world"
    f.close()
    removeFile(path)

  asyncTest "readString partial reads and empty string at EOF":
    let path = tempPath(".txt")
    let f = openAsync(path, fmReadWrite)
    await f.write("abc")
    f.setFilePos(0)
    check (await f.readString(2)) == "ab"
    check (await f.readString(2)) == "c"
    check (await f.readString(2)) == "" # EOF
    # size <= 0 is an empty no-op.
    check (await f.readString(0)) == ""
    f.close()
    removeFile(path)

  asyncTest "readExactlyString fills exactly and raises on early EOF":
    let path = tempPath(".txt")
    let f = openAsync(path, fmReadWrite)
    await f.write("abcde")
    f.setFilePos(0)
    check (await f.readExactlyString(3)) == "abc"
    check (await f.readExactlyString(2)) == "de"
    var failed = false
    try:
      discard await f.readExactlyString(1)
    except AsyncFileIncompleteError:
      failed = true
    check failed
    check (await f.readExactlyString(0)) == ""
    f.close()
    removeFile(path)

  asyncTest "lines iterates every line including empty ones":
    let path = tempPath(".txt")
    let f = openAsync(path, fmReadWrite)
    await f.write("a\n\nb")
    f.setFilePos(0)
    var got: seq[string] = @[]
    f.lines(line):
      got.add(line)
    check got == @["a", "", "b"]
    f.close()
    removeFile(path)

  asyncTest "lines on an empty file runs zero times and supports break":
    let path = tempPath(".txt")
    let f = openAsync(path, fmReadWrite)
    var ran = false
    f.lines(_):
      ran = true
    check not ran
    # break stops the iteration mid-file; the position stays after the
    # consumed line, so a later readLine resumes from there.
    await f.write("x\ny\nz")
    f.setFilePos(0)
    var first = ""
    f.lines(line):
      first = line
      break
    check first == "x"
    check (await f.readLine()) == Opt.some("y")
    f.close()
    removeFile(path)
