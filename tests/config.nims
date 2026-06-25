switch("path", "$projectDir")

when defined(chronosFileUring):
  # The opt-in io_uring backend (`-d:chronosFileUring`, Linux 5.6+) needs iori's
  # async backend selected as chronos.
  switch("define", "asyncBackend=chronos")
