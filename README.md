# chronos_file

Asynchronous file I/O for [chronos](https://github.com/status-im/nim-chronos).

> **POSIX only for now.** Windows (IOCP) is not implemented and fails at compile time.

## Behavior and caveats

- **Regular files are synchronous by default.** epoll/kqueue cannot watch a
  seekable file, so reads/writes run as blocking `pread`/`pwrite` and **block the
  event loop** while the kernel serves them — no concurrency benefit on slow or
  contended storage, mirroring `std/asyncfile`. `flush` is the exception
  (`fsync`/`fdatasync` runs on a worker thread). For true async on regular files,
  enable the [io_uring backend](#io_uring-backend-opt-in) below.
- **Non-seekable fds (pipe / FIFO / tty)** take the truly async `read`/`write` +
  `EAGAIN` path. An fd that is neither seekable nor pollable opens fine, but the
  **first read/write** fails (e.g. `EPERM`), surfacing as `AsyncFileOsError`.
- **One implicit-offset op at a time per handle.** `read`/`write`/`readLine` (and
  the low-level `readBuffer`/`writeBuffer`) share the file position, so a second
  one issued while another is in flight raises `AsyncFileBusyError`. For concurrent
  reads use the positioned `readAt`/`readBufferAt` family: offset-independent and
  never rejected. The positioned writes (`writeAt`/`writeBufferAt`) and positioning
  ops (`setFilePos`/`setFileSize`) must drop the `readLine` read-ahead, so they too
  raise `AsyncFileBusyError` while an implicit-offset op is in flight.
- **Close explicitly** with `close()` (sync) or `closeWait()` (async). A destructor
  releases the fd as a last-resort safety net but does not cancel pending ops.
  `closeWait()` cancels and drains the in-flight op so the awaiter sees
  `CancelledError` rather than `EBADF`; synchronous `close()` cannot, so prefer
  `closeWait()` (or await the op first) when a read/write may still be outstanding.

## Usage

```nim
import pkg/chronos_file

proc main() {.async.} =
  # One-shot helpers: open, transfer and close in a single call.
  await writeFileAsync("/tmp/foo.txt", "test")
  doAssert (await readFileAsync("/tmp/foo.txt")) == "test"        # string
  doAssert (await readFileBytesAsync("/tmp/foo.txt")).len == 4    # seq[byte]

  # Handle API with a guaranteed close (also on error/cancellation).
  # fmReadWriteExisting opens without truncating, so "test" survives.
  withAsyncFile(f, "/tmp/foo.txt", fmReadWriteExisting):
    doAssert (await f.readAllString()) == "test"   # readAll() returns seq[byte]
    f.setFilePos(0)
    await f.write("done")

waitFor main()
```

## io_uring backend (opt-in)

On Linux, regular-file I/O can be routed through **io_uring** instead of the blocking
`pread`/`pwrite` path, so seekable reads/writes (and
`flush`) become **truly asynchronous** and several can be in flight at once.
Append-mode writes are the one exception (see below).

Build with `-d:chronosFileUring` — **opt-in** (off by default) and **Linux 5.6+**
only; the synchronous backend stays the default everywhere else.

```sh
nim c -d:chronosFileUring -d:asyncBackend=chronos yourapp.nim
```

- **Dependency:** needs [`iori`](https://github.com/fox0430/iori) >= 0.2.0
  (`nimble install iori`) with chronos selected as its async backend
  (`-d:asyncBackend=chronos`, as above). Both come from **your** build — a library
  cannot force an async backend on its consumers. `iori` is intentionally not a
  hard dependency (Linux-only, opt-in), so the default build pulls in nothing extra.
- **Graceful fallback:** if io_uring is unavailable at runtime (kernel too old, ring
  setup fails), the library transparently falls back to the synchronous path — the
  define enables the backend, it does not force it.
- **Same public API and contracts.** The single-in-flight rule, positioned `*At`
  concurrency, buffer ownership and cancellation all hold; seekable ops just
  actually suspend now, so cancellation drains the in-flight kernel op before
  settling.
- **Append writes stay synchronous.** io_uring's write takes an explicit offset
  with no "append" mode, and faking `offset = -1` would let `pwrite` ignore
  `O_APPEND` and overwrite. So `fmAppend` writes keep the blocking sequential
  `write` and are never tracked as in-flight for `closeWait` to drain; a large
  append can still stall the loop. Every other seekable write is async.
- **Trade-off:** each op makes a submit → completion round-trip, so a single
  uncontended read/write has *higher* latency than an inline `pread`; the win is
  non-blocking behavior and throughput under concurrency/contention.

## Roadmap

- Thread pools backend

## License

MIT
