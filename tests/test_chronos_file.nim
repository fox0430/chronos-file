## Umbrella test runner for `chronos_file`.
##
## Mirrors `chronos_file.nim`, which re-exports the implementation submodules:
## here we import the per-submodule test files so `nimble test` (which compiles
## only this module) still runs the whole suite. Each `test_posix_*`/`test_common`
## file is also compilable and runnable on its own.

# Each submodule is imported purely for its side effects: importing it runs that
# file's `suite` at module-init time. Nothing is referenced from them, so the
# UnusedImport warning is expected here and silenced.
{.warning[UnusedImport]: off.}

import
  test_posix_handle, test_posix_io, test_posix_flush_close, test_common,
  test_flush_backstop, test_seekable_concurrency
