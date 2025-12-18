use h3_msquic_async::msquic::{Configuration, Registration};
use hyper::Uri;
use hyper::body::Bytes;
use std::sync::Arc;

use crate::client::H3Connector;

#[derive(Clone)]
pub struct H3MsQuicAsyncConnector {
    config: Option<Arc<Configuration>>,
    reg: Option<Arc<Registration>>,
    uri: Uri,
}

impl H3MsQuicAsyncConnector {
    pub fn new(uri: Uri, config: Arc<Configuration>, reg: Arc<Registration>) -> Self {
        Self {
            uri,
            config: Some(config),
            reg: Some(reg),
        }
    }
}

impl H3Connector for H3MsQuicAsyncConnector {
    type CONN = h3_msquic_async::Connection;
    type OS = h3_msquic_async::OpenStreams;
    type SS = h3_msquic_async::SendStream<Bytes>;
    type RS = h3_msquic_async::RecvStream;
    type BS = h3_msquic_async::BidiStream<Bytes>;
    async fn connect(&self) -> Result<Self::CONN, crate::Error> {
        let conn = h3_msquic_async::msquic_async::Connection::new(self.reg.as_ref().unwrap())?;
        conn.start(
            self.config.as_ref().unwrap(),
            self.uri.host().unwrap(),
            self.uri.port_u16().unwrap(),
        )
        .await?;
        let h3_conn = h3_msquic_async::Connection::new(conn);
        Ok(h3_conn)
    }
}

impl Drop for H3MsQuicAsyncConnector {
    fn drop(&mut self) {
        tracing::debug!("H3MsQuicAsyncConnector dropping.");
        self.config.take();
        self.reg.take();
        tracing::debug!("H3MsQuicAsyncConnector dropped.");
    }
}
