//! `h3_util::client::is_clat_stub`: whether a local address names a
//! CLAT/464XLAT translator rather than a real interface.
//!
//! The predicate decides, inside the msquic-async connector, whether an
//! unconnected/shared binding is set up at all — so getting its boundaries
//! wrong costs either direct-path migration on an ordinary network (too wide)
//! or a leg that cannot connect on an IPv6-only one (too narrow).

use std::net::SocketAddr;

use h3_util::client::is_clat_stub;

fn addr(s: &str) -> SocketAddr {
    s.parse().unwrap()
}

/// `192.0.0.0/29` is the whole of it: `.0` through `.7`.
#[test]
fn the_whole_service_continuity_prefix() {
    for last in 0..=7 {
        let a = addr(&format!("192.0.0.{last}:4433"));
        assert!(is_clat_stub(&a), "{a} is in 192.0.0.0/29");
    }
}

/// And nothing past it. `.8` is the first address outside the /29, and it is
/// the one a `== 0` test on the last octet or a /24 would get wrong.
#[test]
fn and_nothing_outside_it() {
    for outside in [
        "192.0.0.8:4433",
        "192.0.0.255:4433",
        "192.0.1.4:4433",
        "192.0.2.1:4433",
        "198.51.100.7:4433",
        "10.0.0.4:4433",
        "127.0.0.1:4433",
    ] {
        let a = addr(outside);
        assert!(!is_clat_stub(&a), "{a} is not in 192.0.0.0/29");
    }
}

/// A v6 socket facing the translator reports `::ffff:192.0.0.4`, and that is
/// still the translator.
#[test]
fn a_v4_mapped_stub_is_a_stub() {
    assert!(is_clat_stub(&addr("[::ffff:192.0.0.4]:4433")));
    assert!(!is_clat_stub(&addr("[::ffff:192.0.2.4]:4433")));
}

/// But a v4-*compatible* address is a different thing entirely. `to_ipv4`
/// unwraps both and would make this match; `to_ipv4_mapped` is why it does
/// not.
#[test]
fn a_v4_compatible_address_is_not() {
    assert!(!is_clat_stub(&addr("[::192.0.0.4]:4433")));
}

/// An ordinary IPv6 address is not a v4 translator stub, and must not be read
/// as one — an IPv6-only host's leg depends on it.
#[test]
fn an_ordinary_v6_address_is_not() {
    for v6 in ["[2001:db8::1]:4433", "[fe80::1]:4433", "[::1]:4433"] {
        assert!(!is_clat_stub(&addr(v6)), "{v6}");
    }
}
