use std::{
    fmt,
    future::Future,
    net::SocketAddr,
    pin::Pin,
    task::{Context, Poll},
};

use futures::future::BoxFuture;
use hyper::{
    Request, Response, Uri,
    body::{Body, Bytes},
    rt::Executor,
};
use tower::{
    Service,
    buffer::{Buffer, future::ResponseFuture as BufferResponseFuture},
    util::BoxService,
};

use crate::client_conn;
use crate::{client_body::H3IncomingClient, executor::SharedExec};

const DEFAULT_BUFFER_SIZE: usize = 1024;

pub trait H3Connector: Send + 'static + Clone {
    type CONN: h3::quic::Connection<
            Bytes,
            OpenStreams = Self::OS,
            SendStream = Self::SS,
            RecvStream = Self::RS,
        > + Send;
    type OS: h3::quic::OpenStreams<Bytes, BidiStream = Self::BS> + Clone + Send; // Clone is needed for cloning send_request
    type SS: h3::quic::SendStream<Bytes> + Send;
    type RS: h3::quic::RecvStream + Send;
    type BS: h3::quic::BidiStream<Bytes, RecvStream = Self::RS, SendStream = Self::SS> + Send;

    fn connect(
        &self,
    ) -> impl std::future::Future<Output = Result<Self::CONN, crate::Error>> + std::marker::Send;
}

/// Use the host:port portion of the uri and resolve to an sockaddr.
/// If uri host portion is an ip string, then directly use the ip addr without
/// dns lookup.
pub async fn dns_resolve(uri: &Uri) -> std::io::Result<Vec<SocketAddr>> {
    let host_port = uri
        .authority()
        .ok_or(std::io::Error::from(std::io::ErrorKind::InvalidInput))?
        .as_str();
    match host_port.parse::<SocketAddr>() {
        Ok(addr) => Ok(vec![addr]),
        Err(_) => {
            // uri is using a dns name. try resolve it and return the first.
            tokio::net::lookup_host(host_port)
                .await
                .map(|a| a.collect::<Vec<_>>())
        }
    }
}

/// The first of `addrs`, in the order given, that this host can route to,
/// with a UDP socket already connected to it -- whose `local_addr()` is the
/// address the OS would send from.
///
/// **Keep the resolver's order and take the first that works.** getaddrinfo
/// sorts by RFC 6724, which puts a family this host cannot use last; on a
/// single-stack host a name with both A and AAAA records therefore resolves
/// with the unusable family at the end. Whether a family is usable is what a
/// UDP `connect` answers: it sends nothing, and fails at once (ENETUNREACH,
/// EHOSTUNREACH) when there is no route.
pub async fn first_routable(
    addrs: impl IntoIterator<Item = SocketAddr>,
) -> Option<(SocketAddr, tokio::net::UdpSocket)> {
    for addr in addrs {
        let bound = if addr.is_ipv6() {
            tokio::net::UdpSocket::bind("[::]:0").await
        } else {
            tokio::net::UdpSocket::bind("0.0.0.0:0").await
        };
        let Ok(udp) = bound else { continue };
        if udp.connect(addr).await.is_ok() {
            return Some((addr, udp));
        }
    }
    None
}

/// Which IP family a connection should prefer when the name resolves to both.
///
/// **A preference, not a requirement** -- see [`prefer_family`]. A caller that
/// knows which family the far side will be using wants that one tried first;
/// it does not want the connection to fail if the answer has gone stale.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AddressFamily {
    V4,
    V6,
}

impl AddressFamily {
    /// Whether `addr` is of this family.
    ///
    /// Compared on the address as it came out of the resolver: an `A` record
    /// is V4 and an `AAAA` is V6. An IPv4-mapped `::ffff:a.b.c.d` counts as V6
    /// here, which is what it is on the wire — resolvers do not hand those
    /// back for a name, so the case does not arise in practice, and guessing
    /// at intent would be the less predictable answer.
    pub fn holds(&self, addr: &SocketAddr) -> bool {
        match self {
            Self::V4 => addr.is_ipv4(),
            Self::V6 => addr.is_ipv6(),
        }
    }
}

/// `addrs` with `family` moved to the front, keeping the resolver's order
/// within each group.
///
/// **Reordering rather than filtering, deliberately.** What a caller wants is
/// "use this family, because the peer is", and what it must not get is a
/// connection that fails because the family it named stopped working since it
/// was chosen. Handing the reordered list to [`first_routable`] gives both in
/// one pass: every address of the preferred family is tried, in order, before
/// any other is — and if none of them routes, the rest still are. Filtering
/// and then falling back to the unfiltered list would have the same outcome
/// and probe twice.
pub fn prefer_family(
    addrs: impl IntoIterator<Item = SocketAddr>,
    family: AddressFamily,
) -> Vec<SocketAddr> {
    // `partition` keeps the relative order inside each half, which is what
    // makes this a reordering and not a re-ranking: RFC 6724 already sorted
    // within a family and that ordering is worth keeping.
    let (preferred, rest): (Vec<_>, Vec<_>) =
        addrs.into_iter().partition(|addr| family.holds(addr));
    preferred.into_iter().chain(rest).collect()
}

/// Whether `addr` falls in `192.0.0.0/29`, the IANA "IPv4 Service Continuity
/// Prefix" (RFC 7335, reassigned from RFC 6333's original DS-Lite
/// reservation). On an IPv6-only network, Android runs a local NAT64
/// translator (`clatd`, "464XLAT"/CLAT) that presents an address in this
/// range so IPv4-only sockets keep working, silently rewriting their traffic
/// onto the real IPv6 uplink; a DS-Lite CPE's B4 address (192.0.0.2) is drawn
/// from the same /29 and behaves the same way for this connector's purposes.
///
/// **Why a connector cares**: a plain connected socket gets associated
/// with the translator's path at `connect()` time and works fine through it.
/// The unconnected/shared-binding socket this connector sets up for direct-
/// path migration is only ever `bind()`-ed and driven with `sendto()` per
/// packet -- and empirically (isekai-link#213: reconnects over an IPv6-only
/// cellular network consistently failed, `ConnectionLost(ShutdownByTransport)`
/// with `QUIC_STATUS_ABORTED`/`QUIC_STATUS_CONNECTION_IDLE`, while an ordinary
/// connected socket to the identical address succeeded every time) that mode
/// does not reliably route through the translator. The exact kernel
/// mechanism is unconfirmed; the address is the one repeatable signal that
/// this is about to happen.
pub fn is_clat_stub(addr: &SocketAddr) -> bool {
    let ip = match addr.ip() {
        std::net::IpAddr::V4(ip) => ip,
        // `to_ipv4_mapped` only unwraps genuine `::ffff:a.b.c.d` addresses --
        // unlike `to_ipv4`, it does not also match IPv4-*compatible*
        // addresses (`::a.b.c.d`), which would make `::192.0.0.4` match too.
        std::net::IpAddr::V6(ip) => match ip.to_ipv4_mapped() {
            Some(ip) => ip,
            None => return false,
        },
    };
    let [a, b, c, d] = ip.octets();
    a == 192 && b == 0 && c == 0 && (d & 0xF8) == 0
}

/// Cloneable http3 client channel, which can be used to enable multiplexing requests.
pub struct H3Channel<C, B>
where
    C: H3Connector,
    B: Body + Send + 'static + Unpin,
    B::Data: Send,
    B::Error: Into<crate::Error> + Send,
{
    #[allow(clippy::type_complexity)]
    svc: Buffer<
        Request<B>,
        BoxFuture<'static, Result<Response<H3IncomingClient<C::RS, Bytes>>, crate::Error>>,
    >,
}

impl<C, B> Clone for H3Channel<C, B>
where
    C: H3Connector,
    B: Body + Send + 'static + Unpin,
    B::Data: Send,
    B::Error: Into<crate::Error> + Send,
{
    fn clone(&self) -> Self {
        Self {
            svc: self.svc.clone(),
        }
    }
}

pub struct ResponseFuture<C>
where
    C: H3Connector,
{
    #[allow(clippy::type_complexity)]
    inner: BufferResponseFuture<
        BoxFuture<'static, Result<Response<H3IncomingClient<C::RS, Bytes>>, crate::Error>>,
    >,
}

impl<C, B> H3Channel<C, B>
where
    C: H3Connector,
    B: Body + Send + 'static + Unpin,
    B::Data: Send,
    B::Error: Into<crate::Error> + Send,
{
    pub fn new(connector: C, uri: Uri, executor: Option<SharedExec>) -> Self {
        let executor = executor.unwrap_or_else(SharedExec::tokio);
        let svc = H3Connection::new(connector, uri, Some(executor.clone()));
        let (svc, worker) = Buffer::pair(svc, DEFAULT_BUFFER_SIZE);
        executor.execute(worker);
        Self { svc }
    }
}

impl<C, B> Service<Request<B>> for H3Channel<C, B>
where
    C: H3Connector,
    B: Body + Send + 'static + Unpin,
    B::Data: Send,
    B::Error: Into<crate::Error> + Send,
{
    type Response = Response<H3IncomingClient<C::RS, Bytes>>;
    type Error = crate::Error;
    type Future = ResponseFuture<C>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Service::poll_ready(&mut self.svc, cx).map_err(crate::Error::from)
    }

    fn call(&mut self, req: Request<B>) -> Self::Future {
        let inner = Service::call(&mut self.svc, req);
        ResponseFuture { inner }
    }
}

impl<C> Future for ResponseFuture<C>
where
    C: H3Connector,
{
    type Output = Result<Response<H3IncomingClient<C::RS, Bytes>>, crate::Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.inner)
            .poll(cx)
            .map_err(crate::Error::from)
    }
}

impl<C, B> fmt::Debug for H3Channel<C, B>
where
    C: H3Connector,
    B: Body + Send + 'static + Unpin,
    B::Data: Send,
    B::Error: Into<crate::Error> + Send,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("H3Channel").finish()
    }
}

impl<C> fmt::Debug for ResponseFuture<C>
where
    C: H3Connector,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ResponseFuture").finish()
    }
}

/// h3 client connection, wrapping inner types for ease of use.
/// All request will be sent to the connection established using the connector.
/// Currently connector can only connect to a fixed server (to support grpc use case).
/// Expand connector to do resolve different server based on uri can be added in future.
pub struct H3Connection<C, B>
where
    C: H3Connector,
    B: Body + Send + 'static + Unpin,
    B::Data: Send,
    B::Error: Into<crate::Error>,
{
    #[allow(clippy::type_complexity)]
    inner: BoxService<Request<B>, Response<H3IncomingClient<C::RS, Bytes>>, crate::Error>,
}

impl<C, B> H3Connection<C, B>
where
    C: H3Connector,
    B: Body + Send + 'static + Unpin,
    B::Data: Send,
    B::Error: Into<crate::Error> + Send,
{
    pub fn new(connector: C, uri: Uri, executor: Option<SharedExec>) -> Self {
        let executor = executor.unwrap_or_else(SharedExec::tokio);
        let sender = client_conn::RequestSender::new(connector, uri, executor);
        Self {
            inner: BoxService::new(sender),
        }
    }
}

impl<C, B> Service<Request<B>> for H3Connection<C, B>
where
    C: H3Connector,
    B: Body + Send + 'static + Unpin,
    B::Data: Send,
    B::Error: Into<crate::Error>,
{
    type Response = Response<H3IncomingClient<C::RS, Bytes>>;
    type Error = crate::Error;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Service::poll_ready(&mut self.inner, cx)
    }

    fn call(&mut self, req: Request<B>) -> Self::Future {
        self.inner.call(req)
    }
}

/// http3 client.
/// Note the client does not do dns resolve but blindly sends requests
/// using connections created by the connector.
/// Used for sending HTTP request directly.
pub struct H3Client<C, B>
where
    C: H3Connector,
    B: Body + Send + 'static + Unpin,
    B::Data: Send,
    B::Error: Into<crate::Error> + Send,
{
    channel: H3Connection<C, B>,
}

impl<C, B> H3Client<C, B>
where
    C: H3Connector,
    B: Body + Send + 'static + Unpin,
    B::Data: Send,
    B::Error: Into<crate::Error> + Send,
{
    pub fn new(inner: H3Connection<C, B>) -> Self {
        Self { channel: inner }
    }

    pub async fn send(
        &mut self,
        req: Request<B>,
    ) -> Result<Response<H3IncomingClient<C::RS, Bytes>>, crate::Error> {
        // wait for ready
        futures::future::poll_fn(|cx| self.channel.poll_ready(cx)).await?;
        self.channel.call(req).await
    }
}
