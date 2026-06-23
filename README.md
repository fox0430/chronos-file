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
- **One implicit-offset op at a time per handle.** `read`/`write`/`readLine` (and
  the low-level `readBuffer`/`writeBuffer`) all advance the shared file position,
  so a second one issued while another is in flight raises `AsyncFileBusyError`.
  For concurrent reads on a single handle use the positioned `readAt`/`readBufferAt`
  family: it is offset-independent, touches no shared state and is never rejected.
  The positioned *writes* (`writeAt`/`writeBufferAt`) and the positioning ops
  (`setFilePos`/`setFileSize`) must drop the `readLine` read-ahead, which touches
  the shared offset, so they too raise `AsyncFileBusyError` while an implicit-offset
  op is in flight. (On non-seekable fds the same error guards against a second
  concurrent `read`, or a second `write`.)
- **Close explicitly** with `close()` (sync) or `closeWait()` (async). A
  destructor releases the fd as a last-resort safety net, but it does not cancel
  pending ops — never drop a handle to the GC while a pipe/FIFO read/write is in
  flight. `closeWait()` cancels and drains the in-flight pipe/FIFO op so the
  awaiter sees `CancelledError` rather than `EBADF`. (Seekable file reads/writes
  complete synchronously today, so none are ever pending at close.)

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
