use std::task::Poll;
use tokio::io::AsyncWrite;

struct NeverWriter;
impl AsyncWrite for NeverWriter {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        // Accept all writes immediately, pretend we wrote everything
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

pub fn writer_or_never<W: AsyncWrite + Unpin + 'static + Send>(
    writer: Option<W>,
) -> Box<dyn AsyncWrite + Unpin + Send> {
    if let Some(writer) = writer {
        Box::new(writer)
    } else {
        Box::new(NeverWriter)
    }
}
