//! `h3_util::client::prefer_family`: which family an unconnected
//! (direct-path) connection tries first when the name resolves to both.
//!
//! **Why a caller would care.** A direct path is opened from the relay leg's
//! own binding, so one leg offers candidates in exactly one family. If the two
//! ends of a session reach the relay over different families they advertise
//! addresses neither can use, and no direct path forms — the session works,
//! over the relay, silently. Being told which family the other end is using is
//! what lets both pick the same one.
//!
//! The broadcast address stands in for "a family this host cannot use", as in
//! `first_routable.rs`: a UDP `connect` to it fails at once (EACCES without
//! SO_BROADCAST), on every host whatever its own IPv6 setup.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

use h3_util::client::{AddressFamily, first_routable, prefer_family};

const UNROUTABLE4: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::BROADCAST), 4433);
const LOOPBACK4: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 4433);
const LOOPBACK6: SocketAddr = SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), 4433);
const DOC4: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 0, 2, 1)), 4433);
const DOC6: SocketAddr = SocketAddr::new(
    IpAddr::V6(Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 1)),
    4433,
);

#[test]
fn the_named_family_goes_first() {
    assert_eq!(
        prefer_family([LOOPBACK4, LOOPBACK6], AddressFamily::V6),
        [LOOPBACK6, LOOPBACK4]
    );
    assert_eq!(
        prefer_family([LOOPBACK6, LOOPBACK4], AddressFamily::V4),
        [LOOPBACK4, LOOPBACK6]
    );
}

/// **The resolver already sorted within a family** (RFC 6724), and that
/// ordering is worth keeping — this moves a group, it does not re-rank inside
/// one.
#[test]
fn order_within_each_family_is_kept() {
    assert_eq!(
        prefer_family([DOC4, LOOPBACK6, LOOPBACK4, DOC6], AddressFamily::V6),
        [LOOPBACK6, DOC6, DOC4, LOOPBACK4]
    );
}

/// Nothing to move: the list comes back as it went in, rather than empty. A
/// filter would have returned nothing here and left the connection with no
/// address at all.
#[test]
fn a_family_that_is_not_there_changes_nothing() {
    assert_eq!(
        prefer_family([LOOPBACK4, DOC4], AddressFamily::V6),
        [LOOPBACK4, DOC4]
    );
    assert_eq!(prefer_family([], AddressFamily::V4), []);
}

#[test]
fn holds_answers_for_both_families() {
    assert!(AddressFamily::V4.holds(&LOOPBACK4));
    assert!(!AddressFamily::V4.holds(&LOOPBACK6));
    assert!(AddressFamily::V6.holds(&LOOPBACK6));
    assert!(!AddressFamily::V6.holds(&LOOPBACK4));
}

/// **What the pair is for.** Without a preference `first_routable` takes the
/// resolver's first answer; with one it takes the named family's — which is
/// the whole point, since the other end has been told the same family.
#[tokio::test]
async fn the_preference_changes_what_first_routable_picks() {
    let addrs = [LOOPBACK4, LOOPBACK6];

    let (unprefixed, _) = first_routable(addrs).await.expect("v4 loopback routes");
    assert_eq!(unprefixed, LOOPBACK4, "the resolver's order, as before");

    let (preferred, _) = first_routable(prefer_family(addrs, AddressFamily::V6))
        .await
        .expect("v6 loopback routes");
    assert_eq!(preferred, LOOPBACK6);
}

/// **A preference, not a requirement.** The named family is first in line and
/// does not route; the connection still gets made on the other one rather than
/// failing. A caller picks a family from a measurement taken earlier, and the
/// route can change in between.
#[tokio::test]
async fn an_unroutable_preferred_family_falls_through() {
    let (picked, _) = first_routable(prefer_family([UNROUTABLE4, LOOPBACK6], AddressFamily::V4))
        .await
        .expect("it must not give up with a routable address left");
    assert_eq!(picked, LOOPBACK6);
}
