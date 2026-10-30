//! `h3_util::client::first_routable`: which resolved address an unconnected
//! (direct-path) connection is pinned to.
//!
//! The broadcast address stands in for "a family this host cannot use": a
//! UDP `connect` to it fails at once (EACCES without SO_BROADCAST), just as
//! one to an unrouted family fails with ENETUNREACH, and it does so on every
//! host, whatever its own IPv6 setup.

use std::net::{Ipv4Addr, SocketAddr};

use h3_util::client::first_routable;

const UNROUTABLE: SocketAddr = SocketAddr::new(std::net::IpAddr::V4(Ipv4Addr::BROADCAST), 4433);
const LOOPBACK4: SocketAddr = SocketAddr::new(std::net::IpAddr::V4(Ipv4Addr::LOCALHOST), 4433);
const LOOPBACK6: SocketAddr =
    SocketAddr::new(std::net::IpAddr::V6(std::net::Ipv6Addr::LOCALHOST), 4433);

/// The resolver's order is kept: with two usable answers, the first one wins.
/// Taking the last (`pop()`) is what put the unusable family first in line on
/// a single-stack host.
#[tokio::test]
async fn takes_the_first_answer_not_the_last() {
    let (addr, _) = first_routable([LOOPBACK4, LOOPBACK6])
        .await
        .expect("loopback routes");
    assert_eq!(addr, LOOPBACK4);
}

/// The single-stack case as getaddrinfo hands it over: RFC 6724 sorts the
/// family the host cannot use *last*. Taking the last answer picked exactly
/// that one, and the leg failed with ENETUNREACH.
#[tokio::test]
async fn an_unusable_family_sorted_last_is_not_chosen() {
    let (addr, _) = first_routable([LOOPBACK4, UNROUTABLE])
        .await
        .expect("loopback routes");
    assert_eq!(addr, LOOPBACK4);
}

/// An answer this host cannot reach is skipped, not fatal: the next one is
/// used. Before, the `connect` failing ended the connection attempt.
#[tokio::test]
async fn skips_an_answer_with_no_route() {
    let (addr, udp) = first_routable([UNROUTABLE, LOOPBACK4])
        .await
        .expect("loopback routes");
    assert_eq!(addr, LOOPBACK4);
    // The local address handed to the connection is one for that answer's
    // family, which is what pinning the remote address to `addr` relies on.
    let local = udp.local_addr().unwrap();
    assert!(local.is_ipv4() && local.ip().is_loopback(), "{local}");
}

/// Nothing routable is `None`, which leaves the connection to the plain
/// connected-socket path (and its QMUX fallback) instead of failing here.
#[tokio::test]
async fn none_when_nothing_routes() {
    assert!(first_routable([UNROUTABLE]).await.is_none());
    assert!(first_routable([]).await.is_none());
}
