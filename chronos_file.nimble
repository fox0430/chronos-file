# Package

version = "0.2.0"
author = "fox0430"
description = "Asynchronous file I/O for chronos"
license = "MIT"

# Dependencies

requires "nim >= 2.0.16"
requires "chronos >= 4.2.0"

# The opt-in io_uring backend (`-d:chronosFileUring`, Linux 5.6+) additionally
# needs the `iori` library; it is deliberately NOT a hard dependency so
# the default (synchronous) build stays dependency-light and cross-platform.

# Tasks

task test, "Run the test suite":
  exec "nim c -r tests/test_chronos_file.nim"

task testUring, "Run the test suite with the io_uring backend":
  # Needs Linux 5.6+ and iori
  exec "nim c -r -d:chronosFileUring tests/test_chronos_file.nim"

task testAll, "Run the all test suite include io_uring backend":
  exec "nim c -r tests/test_chronos_file.nim"
  # Needs Linux 5.6+ and iori
  exec "nim c -r -d:chronosFileUring tests/test_chronos_file.nim"
