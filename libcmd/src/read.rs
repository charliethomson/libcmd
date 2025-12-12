use std::task::Poll;

use tokio::io::AsyncRead;

struct NeverReader;
impl AsyncRead for NeverReader {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        _buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        // Will never have anything to read, so will never activate a select
        Poll::Pending
    }
}

pub fn reader_or_never<R: AsyncRead + Unpin + 'static + Send>(
    reader: Option<R>,
) -> Box<dyn AsyncRead + Unpin + Send> {
    if let Some(reader) = reader {
        Box::new(reader)
    } else {
        Box::new(NeverReader)
    }
}
