# Package

version = "0.1.0"
author = "fox0430"
description = "Asynchronous file I/O for chronos"
license = "MIT"

# Dependencies

requires "nim >= 2.0.16"
requires "chronos >= 4.2.0"

# Tasks

task test, "Run the test suite":
  exec "nim c -r tests/test_chronos_file.nim"
