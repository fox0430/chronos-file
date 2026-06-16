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
