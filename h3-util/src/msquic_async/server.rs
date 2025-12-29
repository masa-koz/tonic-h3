use h3_msquic_async::msquic_async;
use hyper::body::Bytes;
use tokio::sync::mpsc;

use crate::server::H3Acceptor;

pub struct H3MsQuicAsyncAcceptor {
    listener: msquic_async::Listener,
    conn_sender: Option<mpsc::Sender<msquic_async::Connection>>,
}

impl H3MsQuicAsyncAcceptor {
    pub fn new(listener: msquic_async::Listener) -> Self {
        Self {
            listener,
            conn_sender: None,
        }
    }

    pub fn with_channel(mut self, sender: mpsc::Sender<msquic_async::Connection>) -> Self {
        self.conn_sender = Some(sender);
        self
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
                if let Some(sender) = self.conn_sender.as_ref() {
                    sender.send(conn.clone()).await?;
                }
                let h3_conn = h3_msquic_async::Connection::new(conn);
                Ok(Some(h3_conn))
            }
            Err(msquic_async::ListenError::Finished) => Ok(None),
            Err(e) => Err(Box::new(e)),
        }
    }
}
