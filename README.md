# chronos_file

Asynchronous file I/O for [chronos](https://github.com/status-im/nim-chronos).

> **POSIX only for now.** Windows (IOCP) is not implemented and fails at compile time.

## Behavior and caveats

- **Regular files are synchronous under the hood.** epoll/kqueue cannot watch a
  seekable file, so reads/writes run as blocking `pread`/`pwrite` at a tracked
  offset (no `lseek` bookkeeping) and **block the event loop** while the kernel
  serves them — no concurrency benefit on slow or contended storage. This mirrors
  `std/asyncfile`; real async awaits an io_uring/thread-pool backend.
  The exception is `flush`: `fsync`/`fdatasync` runs on a worker thread.
- **Non-seekable fds (pipe / FIFO / tty)** take the truly async `read`/`write` +
  `EAGAIN` path. An fd that is neither seekable nor epoll-pollable opens fine, but
  the **first read/write** fails (e.g. `EPERM`), surfacing as `AsyncFileOsError`.
- **Close explicitly** with `close()` (sync) or `closeWait()` (async). A
  destructor releases the fd as a last-resort safety net, but it does not cancel
  pending ops — never drop a handle to the GC while a pipe/FIFO read/write is in
  flight. `closeWait()` cancels and drains in-flight ops so the awaiter sees
  `CancelledError` rather than `EBADF`.

## Usage

```nim
import pkg/chronos_file

proc main() {.async.} =
  # One-shot helpers: open, transfer and close in a single call.
  await writeFileAsync("/tmp/foo.txt", "test")
  doAssert (await readFileAsync("/tmp/foo.txt")) == "test"

  withAsyncFile(f, "/tmp/foo.txt", fmReadWrite):
    # Handle API with a guaranteed close (also on error/cancellation).
    await f.write("more")
    f.setFilePos(0)
    let data = await f.readAll()           # binary: seq[byte]
    doAssert data == @[byte 'm', byte 'o', byte 'r', byte 'e']
    f.setFilePos(0)
    doAssert (await f.readAllString()) == "more"  # text: string

waitFor main()
```

## Roadmap

- io_uring backend

- Thread pools backend

## License

MIT
