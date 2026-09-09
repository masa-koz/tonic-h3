use h3_msquic_async::{msquic, msquic_async};
use hyper::Uri;
use hyper::body::Bytes;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::{net::UdpSocket, sync::mpsc};

use crate::client::{H3Connector, dns_resolve};

/// Whether `addr` is Android's local NAT64 stub for 464XLAT ("CLAT") --
/// `192.0.0.0/29`, the "IPv4 Service Continuity Prefix" IANA reserves for
/// this in RFC 7600. On an IPv6-only network, Android runs a local translator
/// (`clatd`) that presents this stub so IPv4-only sockets keep working,
/// silently rewriting their traffic onto the real IPv6 uplink.
///
/// **Why this connector cares**: a plain connected socket gets associated
/// with the translator's path at `connect()` time and works fine through it.
/// The unconnected/shared-binding socket this connector sets up for direct-
/// path migration is only ever `bind()`-ed and driven with `sendto()` per
/// packet -- and empirically (isekai-link#213: reconnects over an IPv6-only
/// cellular network consistently failed, `ConnectionLost(ShutdownByTransport)`
/// with `QUIC_STATUS_ABORTED`/`QUIC_STATUS_CONNECTION_IDLE`, while an ordinary
/// connected socket to the identical address succeeded every time) that mode
/// does not reliably route through CLAT's translation. The exact kernel
/// mechanism is unconfirmed; the address is the one repeatable signal that
/// this is about to happen.
fn is_clat_stub(addr: &SocketAddr) -> bool {
    let std::net::IpAddr::V4(ip) = addr.ip() else {
        return false;
    };
    let [a, b, c, d] = ip.octets();
    a == 192 && b == 0 && c == 0 && (d & 0xF8) == 0
}

#[derive(Clone)]
pub struct H3MsQuicAsyncConnector {
    config: Option<Arc<msquic::Configuration>>,
    config_qmux: Option<Arc<msquic::Configuration>>,
    is_unconnected: bool,
    reg: Option<Arc<msquic_async::Registration>>,
    uri: Uri,
    conn_sender: Option<mpsc::Sender<msquic_async::Connection>>,
    peer_certificate: Option<PeerCertificateCallback>,
}

/// Called during the handshake with the peer's certificate, before this
/// connector's connection is started.
///
/// The return status is the verdict, so `Err` fails the handshake. Set with
/// [`H3MsQuicAsyncConnector::with_peer_certificate_callback`].
///
/// The callback the connection itself takes can only be installed before
/// `start`, which happens inside [`H3Connector::connect`] — so a caller that
/// needs to look at the certificate has no way to reach it without this. The
/// channel from `with_channel` hands the connection out *after* the handshake,
/// which is too late.
pub type PeerCertificateCallback = Arc<
    dyn Fn(
            *mut std::ffi::c_void,
            u32,
            msquic::Status,
            *mut std::ffi::c_void,
        ) -> Result<(), msquic::Status>
        + Send
        + Sync,
>;

impl H3MsQuicAsyncConnector {
    pub fn new(
        uri: Uri,
        config: Arc<msquic::Configuration>,
        config_qmux: Option<Arc<msquic::Configuration>>,
        is_unconnected: bool,
        reg: Arc<msquic_async::Registration>,
    ) -> Self {
        Self {
            uri,
            config: Some(config),
            config_qmux,
            is_unconnected,
            reg: Some(reg),
            conn_sender: None,
            peer_certificate: None,
        }
    }

    /// Check the peer's certificate during the handshake.
    ///
    /// The credential has to carry `INDICATE_CERTIFICATE_RECEIVED` for this to
    /// be called at all, and `USE_PORTABLE_CERTIFICATES` for the certificate to
    /// arrive in a form that parses off Windows.
    pub fn with_peer_certificate_callback(mut self, callback: PeerCertificateCallback) -> Self {
        self.peer_certificate = Some(callback);
        self
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
        if let Some(callback) = self.peer_certificate.clone() {
            conn.set_peer_certificate_received_callback(move |cert, flags, status, chain| {
                callback(cert, flags, status, chain)
            });
        }
        if self.is_unconnected {
            // Resolve the address and discover which local address the OS
            // would use to reach it *before* touching the connection's
            // binding mode -- on a CLAT network that address names the local
            // translator rather than a real interface, and unconnected mode
            // does not work through it (see `is_clat_stub`). Deciding first
            // is what lets that case fall through to an ordinary connected
            // socket instead, rather than one already set up to fail.
            let addr = dns_resolve(&self.uri).await?.pop();
            if let Some(addr) = addr {
                let udp = if addr.is_ipv6() {
                    UdpSocket::bind("[::]:0").await?
                } else {
                    UdpSocket::bind("0.0.0.0:0").await?
                };
                udp.connect(addr).await?;
                let local_addr = udp.local_addr()?;
                if is_clat_stub(&local_addr) {
                    // Falls through to the plain `conn.start()` below with
                    // none of the unconnected/shared-binding calls made --
                    // the same path a non-unconnected caller already takes
                    // successfully. Costs direct-path migration for this leg
                    // only, on this network only: the next reconnect (e.g.
                    // back onto WiFi) resolves its own address and decides
                    // again from scratch.
                    tracing::warn!(
                        %addr, %local_addr,
                        "local address is a CLAT/464XLAT stub; falling back to a \
                         connected socket for this leg (no direct-path migration \
                         until the next reconnect off this network)"
                    );
                } else {
                    conn.set_share_binding(true)?;
                    conn.set_unconnected_socket(true)?;
                    conn.set_local_addr(local_addr)?;
                }
            }
        }
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
                if self.config_qmux.is_none() {
                    return Err(e.into());
                }
                let conn = msquic_async::Connection::new_qmux(self.reg.as_ref().unwrap())?;
                // The fallback connection is a different one, so it needs the
                // callback too -- otherwise a peer that makes the first attempt
                // fail gets an unchecked handshake on the second.
                if let Some(callback) = self.peer_certificate.clone() {
                    conn.set_peer_certificate_received_callback(move |cert, flags, status, chain| {
                        callback(cert, flags, status, chain)
                    });
                }
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
