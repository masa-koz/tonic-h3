use h3_msquic_async::{msquic, msquic_async};
use hyper::Uri;
use hyper::body::Bytes;
use std::sync::Arc;
use tokio::sync::mpsc;

use crate::client::{
    AddressFamily, H3Connector, dns_resolve, first_routable, is_clat_stub, prefer_family,
};

/// Which socket mode the connector settled on for one connection, and why.
///
/// **Asking for an unconnected socket is not getting one.** `is_unconnected`
/// is a request, and [`H3Connector::connect`] has three ways of answering it
/// with an ordinary connected socket instead — each of them right for the
/// connection, and each of them silent until now. A caller that only needs to
/// *connect* can ignore this. A caller measuring what a future unconnected
/// connection will be able to do cannot: measured on a connected socket, the
/// answer is about a mode the next leg will not be in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Binding {
    /// An unconnected, shared binding pinned to the address that was picked —
    /// what `is_unconnected` asks for, and the only mode a direct path can be
    /// opened from.
    Unconnected,
    /// An ordinary connected socket, for the reason given.
    Connected(Connected),
}

/// Why a connection ended up on a connected socket.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Connected {
    /// Nothing else was asked for: `is_unconnected` was false.
    AsAsked,
    /// Asked for unconnected, but the local address that would be used is a
    /// CLAT/464XLAT translator stub, which that mode does not route through —
    /// see [`is_clat_stub`](crate::client::is_clat_stub).
    ClatStub,
    /// Asked for unconnected, but no resolved address could be bound and
    /// connected, so there was no local address to pin one to.
    NoRoutableAddress,
    /// The QUIC start failed and the QMUX configuration answered over TCP.
    /// Never an unconnected UDP binding, whatever was asked for — and the
    /// family preference does not reach it either, because that fallback
    /// resolves the host itself.
    QmuxFallback,
}

impl Binding {
    /// Whether this really is an unconnected, shared binding.
    pub fn is_unconnected(&self) -> bool {
        matches!(self, Self::Unconnected)
    }
}

/// One connection the connector established, with the mode it settled on.
///
/// Handed to the channel from [`H3MsQuicAsyncConnector::with_channel`] after
/// the handshake.
#[derive(Clone)]
pub struct Established {
    /// The connection, after its handshake.
    pub conn: msquic_async::Connection,
    /// The socket mode it ended up on — not necessarily the one asked for.
    pub binding: Binding,
}

impl std::fmt::Debug for Established {
    /// Without the connection, which is not `Debug`.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Established")
            .field("binding", &self.binding)
            .finish_non_exhaustive()
    }
}

#[derive(Clone)]
pub struct H3MsQuicAsyncConnector {
    config: Option<Arc<msquic::Configuration>>,
    config_qmux: Option<Arc<msquic::Configuration>>,
    is_unconnected: bool,
    reg: Option<Arc<msquic_async::Registration>>,
    uri: Uri,
    conn_sender: Option<mpsc::Sender<Established>>,
    peer_certificate: Option<PeerCertificateCallback>,
    preferred_family: Option<AddressFamily>,
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
            preferred_family: None,
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

    /// Try this IP family first when the name resolves to both.
    ///
    /// **Only an unconnected connection can honour it**, because only there
    /// does this connector pick the address at all: with `is_unconnected`
    /// false, msquic resolves the host itself in `start` and nothing here is
    /// consulted. That is also the case where it matters — an unconnected
    /// connection is the one a direct path is opened from, and a direct path
    /// needs both ends on the same family to have anything to aim at.
    ///
    /// A preference, not a requirement: if nothing in the named family has a
    /// route, the others are still tried (see
    /// [`prefer_family`](crate::client::prefer_family)). A caller that chose a
    /// family from a measurement taken earlier should not lose the connection
    /// because the route changed since.
    pub fn with_preferred_address_family(mut self, family: AddressFamily) -> Self {
        self.preferred_family = Some(family);
        self
    }

    /// Hand each established connection out, with the [`Binding`] the
    /// connector settled on.
    ///
    /// After the handshake, so it is too late to inspect the certificate —
    /// [`with_peer_certificate_callback`](Self::with_peer_certificate_callback)
    /// is for that.
    pub fn with_channel(mut self, sender: mpsc::Sender<Established>) -> Self {
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
        // Which of the four answers this connection got, reported on the
        // channel below. Every branch that leaves `is_unconnected` unhonoured
        // sets it, so a caller never has to infer the mode from the addresses.
        let mut binding = Binding::Connected(Connected::AsAsked);
        if self.is_unconnected {
            // Resolve the address and discover which local address the OS
            // would use to reach it *before* touching the connection's
            // binding mode -- on a CLAT network that address names the local
            // translator rather than a real interface, and unconnected mode
            // does not work through it (see `is_clat_stub`). Deciding first
            // is what lets that case fall through to an ordinary connected
            // socket instead, rather than one already set up to fail.
            //
            // Which address, and the local one with it, is `first_routable`'s
            // answer: the resolver's order, first address that has a route.
            // This used to be the *last* answer (`pop()`), which on a
            // single-stack host is exactly the family it cannot use whenever
            // the name has both A and AAAA -- and the `connect` failing there
            // ended the leg before the QMUX fallback could run.
            let addrs = dns_resolve(&self.uri).await?;
            // The caller's family, if it named one, goes to the front -- so it
            // is what gets used whenever it still routes, and the others are
            // the fallback rather than the failure.
            let addrs = match self.preferred_family {
                Some(family) => prefer_family(addrs, family),
                None => addrs,
            };
            let picked = first_routable(addrs).await;
            if let Some((addr, udp)) = picked {
                let local_addr = udp.local_addr()?;
                if is_clat_stub(&local_addr) {
                    binding = Binding::Connected(Connected::ClatStub);
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
                    binding = Binding::Unconnected;
                    conn.set_share_binding(true)?;
                    conn.set_unconnected_socket(true)?;
                    conn.set_local_addr(local_addr)?;
                    // The local address was chosen for `addr`'s family, so
                    // the connection must go to `addr` too. Left to resolve
                    // the host itself in `start`, msquic makes its own pick,
                    // which need not be this one: with A and AAAA records a
                    // v4 local address ended up facing a v6 peer, and the
                    // handshake timed out without a packet on the wire.
                    conn.set_remote_addr(addr)?;
                }
            } else {
                // **Said out loud, where it used to be silent.** Nothing could
                // be bound and connected, so there is no local address to pin
                // and this falls through to the plain `start()` below with
                // msquic resolving the host itself -- which means the family
                // preference is not applied either. The addresses it ends up
                // on look entirely ordinary, so without this line the mode is
                // invisible to a caller and to whoever reads the log.
                binding = Binding::Connected(Connected::NoRoutableAddress);
                tracing::warn!(
                    uri = %self.uri,
                    "no resolved address could be bound and connected; falling back to a \
                     connected socket and letting msquic resolve the host (no direct-path \
                     migration, and the address family preference is not applied)"
                );
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
                    conn.set_peer_certificate_received_callback(
                        move |cert, flags, status, chain| callback(cert, flags, status, chain),
                    );
                }
                conn.start(
                    self.config_qmux.as_ref().unwrap(),
                    self.uri.host().unwrap(),
                    self.uri.port_u16().unwrap_or(443),
                )
                .await?;
                // TCP, so whatever was asked for upstairs no longer applies.
                binding = Binding::Connected(Connected::QmuxFallback);
                conn
            }
        };
        if let Some(sender) = self.conn_sender.as_ref() {
            sender
                .send(Established {
                    conn: conn.clone(),
                    binding,
                })
                .await?;
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
