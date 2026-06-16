## Tests for `chronos_file/common`: the `AsyncFile` destructor safety net and
## the inert default-constructed handle (the `opened` flag). Mirrors what lives
## in the `common` submodule.

from std/posix import fcntl, F_GETFD

import pkg/chronos/unittest2/asynctests

import ../chronos_file

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

  asyncTest "dropping handles without close does not leak fds (M1)":
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
