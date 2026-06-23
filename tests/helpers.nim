## Shared helpers for the `chronos_file` test submodules.
##
## Split out alongside the per-submodule test files (`test_common`,
## `test_posix_handle`, `test_posix_io`, `test_posix_flush_close`) so each can
## `import ./helpers` for the temp-path and byte-conversion utilities. The
## `counter` lives here, so the temp paths stay unique across every submodule
## that shares it.

import std/[os, strutils]
export os, strutils

var counter = 0

proc tempPath*(suffix: string): string =
  inc counter
  getTempDir() / ("chronos_file_test_" & $counter & suffix)

proc toBytes*(s: string): seq[byte] =
  result = newSeq[byte](s.len)
  for i, c in s:
    result[i] = byte(c)

template withFakeFile*(f, ctor, body: untyped) =
  ## Run `body` against a hand-built invalid-fd `AsyncFile` (`ctor`, bound to `f`),
  ## always clearing `opened` afterwards so the destructor never closes/unregister2
  ## the fake fd while unwinding. (Unregistering an fd that was never registered
  ## aborts the dispatcher, so were a regression to make `body` raise, an unwinding
  ## destructor on a still-`opened` handle would mask the test failure.)
  ##
  ## Used by the white-box peek/refill-failure tests, which need a handle in a
  ## specific buffer/pushback state but with no real descriptor. `ctor`/`body` are
  ## untyped and expand at the call site, so this needs no `chronos_file` import.
  var f = ctor
  try:
    body
  finally:
    f.opened = false
