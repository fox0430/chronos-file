## Tests for `chronos_file/posix_handle`: `openAsync`/`newAsyncFile`, the
## seek/size accessors (`getFileSize`/`getFilePos`/`setFilePos`/`setFileSize`)
## and the lifecycle guards that live in that submodule.

from std/posix import
  mkfifo, Mode, open, O_RDONLY, O_WRONLY, O_RDWR, O_APPEND, O_NONBLOCK, fcntl, F_GETFD,
  F_GETFL, FD_CLOEXEC

import pkg/chronos/unittest2/asynctests

import ../chronos_file
import helpers

suite "chronos_file: posix_handle (open, positioning, lifecycle)":
  teardown:
    checkLeaks()

  asyncTest "getFileSize / setFileSize / getFilePos / setFilePos":
    let path = tempPath(".bin")
    let f = openAsync(path, fmReadWrite)
    await f.write(@[byte 0, 1, 2, 3, 4, 5, 6, 7])
    check f.getFileSize() == 8
    check f.getFilePos() == 8
    f.setFilePos(3)
    check f.getFilePos() == 3
    f.setFileSize(4)
    check f.getFileSize() == 4
    check f.getFilePos() == 3
    f.close()
    removeFile(path)

  asyncTest "fmAppend positions offset at end":
    let path = tempPath(".bin")
    block:
      let f = openAsync(path, fmWrite)
      await f.write(@[byte 1, 2, 3])
      f.close()
    let f = openAsync(path, fmAppend)
    check f.getFilePos() == 3
    await f.write(@[byte 4, 5])
    f.close()
    let r = openAsync(path, fmRead)
    check (await r.readAll()) == @[byte 1, 2, 3, 4, 5]
    r.close()
    removeFile(path)

  asyncTest "openAsync honours the permission argument":
    let path = tempPath(".bin")
    let f = openAsync(path, fmWrite, {fpUserRead, fpUserWrite})
    await f.write(@[byte 1])
    f.close()
    check getFilePermissions(path) == {fpUserRead, fpUserWrite}
    removeFile(path)

  asyncTest "newAsyncFile wraps an already-open descriptor":
    let path = tempPath(".bin")
    block:
      let f = openAsync(path, fmWrite)
      await f.write(@[byte 7, 8, 9])
      f.close()
    let raw = posix.open(cstring(path), O_RDONLY)
    check raw != -1
    let f = newAsyncFile(AsyncFD(raw))
    check (await f.readAll()) == @[byte 7, 8, 9]
    f.close()
    removeFile(path)

  asyncTest "newAsyncFile detects O_APPEND and rejects positioned writes":
    let path = tempPath(".bin")
    block:
      let f = openAsync(path, fmWrite)
      await f.write(@[byte 1, 2, 3])
      f.close()
    let raw = posix.open(cstring(path), O_WRONLY or O_APPEND)
    check raw != -1
    let f = newAsyncFile(AsyncFD(raw))
    check f.getFilePos() == 3
    var failed = false
    try:
      await f.writeAt(0, @[byte 9])
    except AsyncFileError:
      failed = true
    check failed
    f.close()
    removeFile(path)

  asyncTest "fmReadWriteExisting opens without truncating":
    let path = tempPath(".bin")
    block:
      let f = openAsync(path, fmWrite)
      await f.write(@[byte 1, 2, 3, 4])
      f.close()
    let f = openAsync(path, fmReadWriteExisting)
    check f.getFileSize() == 4
    check (await f.readAll()) == @[byte 1, 2, 3, 4]
    f.close()
    removeFile(path)

  asyncTest "openAsync on a missing file raises AsyncFileOsError":
    let path = tempPath(".missing")
    removeFile(path)
    var raised = false
    try:
      discard openAsync(path, fmRead)
    except AsyncFileOsError:
      raised = true
    check raised

  asyncTest "setFileSize extends the file with zero bytes":
    let path = tempPath(".bin")
    let f = openAsync(path, fmReadWrite)
    await f.write(@[byte 1, 2, 3])
    f.setFileSize(6)
    check f.getFileSize() == 6
    f.setFilePos(0)
    check (await f.readAll()) == @[byte 1, 2, 3, 0, 0, 0]
    f.close()
    removeFile(path)

  asyncTest "setFilePos on a non-seekable FIFO raises":
    let path = tempPath(".fifo")
    check mkfifo(cstring(path), Mode(0o600)) == 0
    let r = openAsync(path, fmRead)
    let w = openAsync(path, fmWrite)
    var failed = false
    try:
      r.setFilePos(0)
    except AsyncFileError:
      failed = true
    check failed
    r.close()
    w.close()
    removeFile(path)

  asyncTest "setFilePos rejects a negative position (L2)":
    let path = tempPath(".bin")
    let f = openAsync(path, fmReadWrite)
    var failed = false
    try:
      f.setFilePos(-1)
    except AsyncFileError:
      failed = true
    check failed
    f.close()
    removeFile(path)

  asyncTest "newAsyncFile wraps a non-seekable FIFO and takes the async path (T1)":
    # Only regular files were covered before; this exercises wrapping a
    # non-seekable fd, where newAsyncFile must set O_NONBLOCK + FD_CLOEXEC and
    # register it so the EAGAIN -> addReader2 path works.
    let path = tempPath(".fifo")
    check mkfifo(cstring(path), Mode(0o600)) == 0
    # Open the FIFO read+write (succeeds without blocking) and crucially WITHOUT
    # O_NONBLOCK, so we can confirm newAsyncFile turns it on.
    let rawR = posix.open(cstring(path), O_RDWR)
    check rawR != -1
    check (fcntl(rawR, F_GETFL) and O_NONBLOCK) == 0 # not set yet
    let r = newAsyncFile(AsyncFD(rawR))
    # newAsyncFile must have made it non-blocking and close-on-exec.
    check (fcntl(rawR, F_GETFL) and O_NONBLOCK) == O_NONBLOCK
    check (fcntl(rawR, F_GETFD) and FD_CLOEXEC) == FD_CLOEXEC
    # The wrapped fd takes the EAGAIN -> addReader2 path: a read with nothing
    # buffered suspends, then completes once a writer feeds it.
    let w = openAsync(path, fmWrite)
    let rfut = r.read(5)
    check not rfut.finished()
    await w.write(@[byte 1, 2, 3, 4, 5])
    check (await rfut) == @[byte 1, 2, 3, 4, 5]
    r.close()
    w.close()
    removeFile(path)

  asyncTest "isOpen / isClosed track the handle lifecycle":
    let path = tempPath(".bin")
    let f = openAsync(path, fmWrite)
    check f.isOpen()
    check not f.isClosed()
    f.close()
    check not f.isOpen()
    check f.isClosed()

    # closeWait flips the state the same way.
    let g = openAsync(path, fmRead)
    check g.isOpen()
    await g.closeWait()
    check not g.isOpen()
    check g.isClosed()

    # A default-constructed handle was never opened: inert, neither open nor
    # closed.
    let inert = AsyncFile()
    check not inert.isOpen()
    check not inert.isClosed()
    removeFile(path)

  asyncTest "openAsync accepts an octal int perm":
    let path = tempPath(".bin")
    let f = openAsync(path, fmWrite, 0o600)
    await f.write(@[byte 1])
    f.close()
    check getFilePermissions(path) == {fpUserRead, fpUserWrite}
    removeFile(path)

  asyncTest "openAsync rejects an out-of-range int perm":
    let path = tempPath(".bin")
    template rejects(body: untyped) =
      var failed = false
      try:
        body
      except AsyncFileError:
        failed = true
      check failed

    rejects:
      discard openAsync(path, fmWrite, -1)
    rejects:
      discard openAsync(path, fmWrite, 0o10000)
