## Tests for `chronos_file/common`: the `AsyncFile` destructor safety net, the
## inert default-constructed handle (the `opened` flag) and the io_uring error
## boundary (`uringResult`/`mapUringErrors`). Mirrors what lives in the
## `common` submodule.

from std/posix import fcntl, F_GETFD
import std/strutils

import pkg/chronos/unittest2/asynctests

import ../chronos_file
import ../chronos_file/common # uringResult / mapUringErrors (internal)

import helpers

proc openAndDrop(path: string) =
  ## Opens a file and lets the handle go out of scope without `close`, so the
  ## destructor must release the fd. Used by the fd-leak safety-net test.
  let f = openAsync(path, fmReadWrite)
  discard f.getFilePos()

proc dropDefaultAsyncFile() =
  ## Constructs a default (never-opened) `AsyncFile()` and lets it go out of
  ## scope, so its destructor runs on an inert handle. It must not touch fd 0
  ## (stdin) nor abort unregistering an fd that was never registered.
  let f = AsyncFile()
  discard f

suite "chronos_file: common (destructor & inert handle)":
  teardown:
    checkLeaks()

  asyncTest "default-constructed AsyncFile is inert (no crash, ops rejected) (#1)":
    # A never-opened handle owns no fd. Dropping it must not close stdin (fd 0)
    # nor abort in the dispatcher unregistering an fd that was never registered.
    dropDefaultAsyncFile()
    # `GC_fullCollect` is declared as possibly raising `Exception`, which is
    # broader than the async proc's raises list; the collect itself cannot fail
    # here, so cast the effect away.
    {.cast(raises: []).}:
      GC_fullCollect()
    check fcntl(0, F_GETFD) != -1 # stdin is still open

    let g = AsyncFile()
    template rejects(body: untyped) =
      var failed = false
      try:
        body
      except AsyncFileError:
        failed = true
      check failed

    rejects:
      discard await g.read(1)
    rejects:
      discard g.getFilePos()
    rejects:
      g.setFilePos(0)
    # close / closeWait are safe no-ops on an inert handle (no raise, no crash).
    g.close()
    await g.closeWait()

  asyncTest "dropping handles without close does not leak fds":
    # The destructor is a best-effort safety net: opening many files and letting
    # each handle go out of scope without close() must not exhaust descriptors.
    let path = tempPath(".bin")
    for i in 0 ..< 4000:
      openAndDrop(path)
    # If fds leaked we would have hit EMFILE above; reaching here means the
    # destructor reclaimed them. A final explicit open/close still works.
    let f = openAsync(path, fmReadWrite)
    await f.write(@[byte 1])
    f.close()
    removeFile(path)

suite "chronos_file: common (io_uring error)":
  # Pure synchronous helpers — no event loop or fd needed. They build the errno
  # -> AsyncFileError boundary the io_uring seam will use once iori is wired in,
  # and they translate the failure shapes iori surfaces today: a negative CQE
  # result (`-errno`) and a raw `IOError`/`OSError` thrown at submission time.

  test "uringResult returns the byte count for a non-negative result":
    check uringResult(0'i32) == 0 # EOF / zero-length write
    check uringResult(4096'i32, "readSeekable") == 4096

  test "uringResult maps a negative result to AsyncFileOsError(-errno)":
    var raised = false
    try:
      discard uringResult(-int32(EBADF), "readSeekable")
    except AsyncFileOsError as e:
      raised = true
      check e.code == EBADF # OSErrorCode preserved
      check "readSeekable" in e.msg # context prefixed, like doPread/doPwrite
    check raised

  test "uringResult negative result carries the OS code, not a bare error":
    # AsyncFileOsError is a subtype of AsyncFileError; the contract is that the
    # OS-coded variant (with OSErrorCode) is raised, not the base type.
    var code = OSErrorCode(0)
    try:
      discard uringResult(-int32(EIO))
    except AsyncFileOsError as e:
      code = e.code
    check code == EIO

  test "mapUringErrors passes a value through when body does not raise":
    let n = mapUringErrors("readSeekable"):
      uringResult(7'i32)
    check n == 7

  test "mapUringErrors surfaces a CQE errno (via uringResult) in-contract":
    var code = OSErrorCode(0)
    try:
      discard mapUringErrors("readSeekable"):
        uringResult(-int32(ENOSPC))
    except AsyncFileOsError as e:
      code = e.code
    check code == ENOSPC

  test "mapUringErrors wraps iori IOError (SQ full / closed) as AsyncFileError":
    # iori fails the bridge future with a bare IOError for ring-state failures;
    # those have no errno, so they map to the base AsyncFileError, not the
    # OS-coded variant. The context is prefixed and the original message kept.
    var msg = ""
    var isOsError = false
    try:
      mapUringErrors("writeSeekable"):
        raise newException(IOError, "io_uring SQ full")
    except AsyncFileOsError:
      isOsError = true
    except AsyncFileError as e:
      msg = e.msg
    check not isOsError
    check "writeSeekable" in msg
    check "SQ full" in msg

  test "mapUringErrors wraps OSError as AsyncFileOsError preserving the code":
    var code = OSErrorCode(0)
    try:
      mapUringErrors("writeSeekable"):
        raise (ref OSError)(errorCode: int32(EACCES), msg: "denied")
    except AsyncFileOsError as e:
      code = e.code
    check code == EACCES

  test "mapUringErrors lets an in-contract AsyncFileError pass through unwrapped":
    # A failure already in chronos-file's hierarchy (e.g. raised by the seam
    # itself) must not be double-wrapped or downgraded to a bare AsyncFileError.
    var code = OSErrorCode(0)
    try:
      mapUringErrors("readSeekable"):
        raise newAsyncFileOsError(EBADF, "readSeekable")
    except AsyncFileOsError as e:
      code = e.code
    check code == EBADF

  test "mapUringErrors lets CancelledError propagate (cancellation is the seam's)":
    var cancelled = false
    try:
      mapUringErrors("readSeekable"):
        raise newException(CancelledError, "cancelled")
    except CancelledError:
      cancelled = true
    check cancelled
