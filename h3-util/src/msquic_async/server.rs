use hyper::body::Bytes;

use crate::server::H3Acceptor;

pub struct H3MsQuicAsyncAcceptor {
    listener: h3_msquic_async::msquic_async::Listener,
}

impl H3MsQuicAsyncAcceptor {
    pub fn new(listener: h3_msquic_async::msquic_async::Listener) -> Self {
        Self {
            listener,
        }
    }
}

impl H3Acceptor for H3MsQuicAsyncAcceptor {
    type CONN = h3_msquic_async::Connection;
    type OS = h3_msquic_async::OpenStreams;
    type SS = h3_msquic_async::SendStream<Bytes>;
    type RS = h3_msquic_async::RecvStream;
    type BS = h3_msquic_async::BidiStream<Bytes>;

    async fn accept(&mut self) -> Result<Option<Self::CONN>, crate::Error> {
        match self.listener.accept().await {
            Ok(conn) => {
                let h3_conn = h3_msquic_async::Connection::new(conn);
                Ok(Some(h3_conn))
            }
            Err(h3_msquic_async::msquic_async::ListenError::Finished) => {
                Ok(None)
            }
            Err(e) => Err(Box::new(e)),
        }
    }
}
