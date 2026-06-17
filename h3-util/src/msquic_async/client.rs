use h3_msquic_async::{msquic, msquic_async};
use hyper::Uri;
use hyper::body::Bytes;
use std::sync::Arc;
use tokio::sync::mpsc;

use crate::client::H3Connector;

#[derive(Clone)]
pub struct H3MsQuicAsyncConnector {
    config: Option<Arc<msquic::Configuration>>,
    config_qmux: Option<Arc<msquic::Configuration>>,
    reg: Option<Arc<msquic::Registration>>,
    uri: Uri,
    conn_sender: Option<mpsc::Sender<msquic_async::Connection>>,
}

impl H3MsQuicAsyncConnector {
    pub fn new(
        uri: Uri,
        config: Arc<msquic::Configuration>,
        config_qmux: Arc<msquic::Configuration>,
        reg: Arc<msquic::Registration>,
    ) -> Self {
        Self {
            uri,
            config: Some(config),
            config_qmux: Some(config_qmux),
            reg: Some(reg),
            conn_sender: None,
        }
    }

    pub fn with_channel(mut self, sender: mpsc::Sender<msquic_async::Connection>) -> Self {
        self.conn_sender = Some(sender);
        self
    }
}

impl H3Connector for H3MsQuicAsyncConnector {
    type CONN = h3_msquic_async::Connection;
    type OS = h3_msquic_async::OpenStreams;
    type SS = h3_msquic_async::SendStream<Bytes>;
    type RS = h3_msquic_async::RecvStream;
    type BS = h3_msquic_async::BidiStream<Bytes>;
    async fn connect(&self) -> Result<Self::CONN, crate::Error> {
        let conn = msquic_async::Connection::new(self.reg.as_ref().unwrap())?;
        conn.set_share_binding(true)?;
        let conn = match conn
            .start(
                self.config.as_ref().unwrap(),
                self.uri.host().unwrap(),
                self.uri.port_u16().unwrap_or(443),
            )
            .await
        {
            Ok(_) => conn,
            Err(e) => {
                tracing::error!("Failed to start QUIC connection: {:?}", e);
                let conn = msquic_async::Connection::new_qmux(self.reg.as_ref().unwrap())?;
                conn.start(
                    self.config_qmux.as_ref().unwrap(),
                    self.uri.host().unwrap(),
                    self.uri.port_u16().unwrap_or(443),
                )
                .await?;
                conn
            }
        };
        if let Some(sender) = self.conn_sender.as_ref() {
            sender.send(conn.clone()).await?;
        }
        let h3_conn = h3_msquic_async::Connection::new(conn);
        Ok(h3_conn)
    }
}

impl Drop for H3MsQuicAsyncConnector {
    fn drop(&mut self) {
        tracing::debug!("H3MsQuicAsyncConnector dropping.");
        self.config.take();
        self.config_qmux.take();
        self.reg.take();
        tracing::debug!("H3MsQuicAsyncConnector dropped.");
    }
}
