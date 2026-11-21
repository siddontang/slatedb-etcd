//! Coalesce the small writes of one HTTP/2 connection into fewer socket writes.
//!
//! h2 0.4 copies a DATA payload into its frame buffer only when the payload is at most
//! 256 bytes on vectored transports, or 1024 bytes on others. A larger payload is chained
//! after the buffer, and the buffer cannot take another frame until it has been written.
//! An etcd Range reply with one 256-byte value is about 350 bytes, so each reply flushed
//! twice: once for HEADERS and DATA, and once for the trailers. Replies of other streams
//! on the same connection could not join either write.
//!
//! `Coalesce` reports a non-vectored transport, so h2 copies payloads up to 1024 bytes and
//! writes one buffer per flush. It holds small writes until the next large write or flush,
//! and sends them with that write in one `writev`. A chained large payload therefore still
//! leaves in one system call together with its frame header.
//!
//! Every write is accepted only into `pending` or the socket. `poll_flush` and
//! `poll_shutdown` send `pending` first, so a flush still means that all accepted bytes
//! reached the socket. hyper's HTTP/1 and HTTP/2 writers always flush after they write.

use std::io::{self, IoSlice};
use std::pin::Pin;
use std::task::{ready, Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

/// Bytes that `Coalesce` can hold before it writes them to the socket.
pub(crate) const PENDING_LIMIT: usize = 8 * 1024;

pub(crate) struct Coalesce<T> {
    inner: T,
    pending: Vec<u8>,
    /// Bytes at the front of `pending` that the socket already took.
    written: usize,
}

impl<T> Coalesce<T> {
    pub(crate) fn new(inner: T) -> Self {
        Self {
            inner,
            pending: Vec::new(),
            written: 0,
        }
    }

    #[cfg(test)]
    pub(crate) fn get_ref(&self) -> &T {
        &self.inner
    }

    fn unsent(&self) -> &[u8] {
        &self.pending[self.written..]
    }

    /// Record that the socket took `n` bytes of `pending`.
    fn advance(&mut self, n: usize) {
        self.written += n;
        if self.written == self.pending.len() {
            self.pending.clear();
            self.written = 0;
        }
    }
}

impl<T: AsyncWrite + Unpin> Coalesce<T> {
    /// Write all of `pending` to the socket.
    fn poll_drain(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        while !self.unsent().is_empty() {
            let n =
                ready!(Pin::new(&mut self.inner).poll_write(cx, &self.pending[self.written..]))?;
            if n == 0 {
                return Poll::Ready(Err(io::ErrorKind::WriteZero.into()));
            }
            self.advance(n);
        }
        Poll::Ready(Ok(()))
    }
}

impl<T: AsyncRead + Unpin> AsyncRead for Coalesce<T> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl<T: AsyncWrite + Unpin> AsyncWrite for Coalesce<T> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }
        if this.pending.len() + buf.len() <= PENDING_LIMIT {
            this.pending.extend_from_slice(buf);
            return Poll::Ready(Ok(buf.len()));
        }
        // Send the held bytes and this write together. Report only bytes of `buf`.
        loop {
            if this.unsent().is_empty() {
                return Pin::new(&mut this.inner).poll_write(cx, buf);
            }
            let held = this.unsent().len();
            let slices = [
                IoSlice::new(&this.pending[this.written..]),
                IoSlice::new(buf),
            ];
            let n = ready!(Pin::new(&mut this.inner).poll_write_vectored(cx, &slices))?;
            if n == 0 {
                return Poll::Ready(Err(io::ErrorKind::WriteZero.into()));
            }
            if n >= held {
                this.advance(held);
                if n > held {
                    return Poll::Ready(Ok(n - held));
                }
            } else {
                this.advance(n);
            }
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        ready!(this.poll_drain(cx))?;
        Pin::new(&mut this.inner).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        ready!(this.poll_drain(cx))?;
        Pin::new(&mut this.inner).poll_shutdown(cx)
    }

    /// h2 copies small DATA payloads into one buffer only on non-vectored transports.
    fn is_write_vectored(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    /// Count the writes that reach the transport.
    #[derive(Default)]
    struct Counting {
        data: Vec<u8>,
        writes: usize,
        /// Accept at most this many bytes per write.
        limit: Option<usize>,
        /// Return `Pending` before every other write.
        stall: bool,
        stalled: bool,
    }

    impl AsyncWrite for Counting {
        fn poll_write(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            self.poll_write_vectored(cx, &[IoSlice::new(buf)])
        }

        fn poll_write_vectored(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            bufs: &[IoSlice<'_>],
        ) -> Poll<io::Result<usize>> {
            if self.stall {
                self.stalled = !self.stalled;
                if self.stalled {
                    cx.waker().wake_by_ref();
                    return Poll::Pending;
                }
            }
            let mut budget = self.limit.unwrap_or(usize::MAX);
            let mut n = 0;
            for buf in bufs {
                let take = buf.len().min(budget);
                self.data.extend_from_slice(&buf[..take]);
                budget -= take;
                n += take;
            }
            self.writes += 1;
            Poll::Ready(Ok(n))
        }

        fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn is_write_vectored(&self) -> bool {
            true
        }
    }

    #[tokio::test]
    async fn small_writes_leave_in_one_write_at_flush() {
        let mut io = Coalesce::new(Counting::default());
        io.write_all(b"headers").await.unwrap();
        io.write_all(b"data").await.unwrap();
        io.write_all(b"trailers").await.unwrap();
        assert_eq!(io.get_ref().writes, 0);
        io.flush().await.unwrap();
        assert_eq!(io.get_ref().writes, 1);
        assert_eq!(io.get_ref().data, b"headersdatatrailers");
        io.flush().await.unwrap();
        assert_eq!(io.get_ref().writes, 1);
    }

    #[tokio::test]
    async fn a_large_write_carries_the_held_bytes() {
        let mut io = Coalesce::new(Counting::default());
        let large = vec![7; PENDING_LIMIT + 1];
        io.write_all(b"frame header").await.unwrap();
        io.write_all(&large).await.unwrap();
        assert_eq!(io.get_ref().writes, 1);
        io.flush().await.unwrap();
        assert_eq!(io.get_ref().writes, 1);
        let mut expected = b"frame header".to_vec();
        expected.extend_from_slice(&large);
        assert_eq!(io.get_ref().data, expected);
    }

    #[tokio::test]
    async fn partial_and_pending_writes_keep_byte_order() {
        for (limit, stall) in [(Some(3), false), (Some(5000), true), (None, true)] {
            let mut io = Coalesce::new(Counting {
                limit,
                stall,
                ..Default::default()
            });
            let mut expected = vec![];
            for round in 0..40u32 {
                let chunk: Vec<u8> = (0..(round * 397) % 12_000)
                    .map(|i| (i ^ round) as u8)
                    .collect();
                io.write_all(&chunk).await.unwrap();
                expected.extend_from_slice(&chunk);
                if round % 7 == 0 {
                    io.flush().await.unwrap();
                    assert_eq!(io.get_ref().data, expected);
                }
            }
            io.shutdown().await.unwrap();
            assert_eq!(io.get_ref().data, expected, "limit {limit:?} stall {stall}");
        }
    }

    #[tokio::test]
    async fn reads_pass_through() {
        let (client, server) = tokio::io::duplex(64);
        let mut server = Coalesce::new(server);
        let mut client = Coalesce::new(client);
        client.write_all(b"ping").await.unwrap();
        client.flush().await.unwrap();
        let mut buf = [0; 4];
        server.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"ping");
    }
}
