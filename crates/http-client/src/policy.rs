use std::fmt::Display;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

use reqwest::Url;

#[derive(Debug, Clone)]
pub struct IpPolicy {
    deny_local_ips: bool,
    allow_list: Vec<cidr::IpCidr>,
}

impl IpPolicy {
    pub fn deny_all_local_ips() -> Self {
        Self::deny_local_ips(Default::default())
    }

    /// Deny local IPs, **except IPs in the `exceptions` list**
    pub fn deny_local_ips(exceptions: Vec<cidr::IpCidr>) -> Self {
        Self { deny_local_ips: true, allow_list: exceptions }
    }

    /// Allow all IPs
    pub fn danger_always_allow() -> Self {
        Self { deny_local_ips: false, allow_list: Default::default() }
    }

    /// Checks if the hostname is a direct URL, and if so performs [`Self::check_ip`] on it.
    pub fn check_ip_in_hostname(&self, url: &Url) -> Result<(), Error> {
        let Some(host) = url.host_str() else { return Ok(()) };
        // we want to use a parsing similar to reqwest's, so we're not directly parsing as a `IpAddr`
        let ip_addr_v4: Option<Ipv4Addr> = host.parse().ok();
        if let Some(ip_addr_v4) = ip_addr_v4 {
            self.check_ip(IpAddr::V4(ip_addr_v4))?;
        }
        let ip_addr_v6: Option<Ipv6Addr> = host.parse().ok();
        if let Some(ip_addr_v6) = ip_addr_v6 {
            self.check_ip(IpAddr::V6(ip_addr_v6))?;
        }
        Ok(())
    }

    pub fn check_socket_addr(&self, addr: SocketAddr) -> Result<(), Error> {
        self.check_ip(addr.ip())
    }

    pub fn check_ip(&self, addr: IpAddr) -> Result<(), Error> {
        // 1. do we deny global IPs?
        if !self.deny_local_ips {
            return Ok(());
        }

        // 2. Is the IP global?
        let is_global = match addr {
            IpAddr::V4(ipv4_addr) => is_global_4(ipv4_addr),
            IpAddr::V6(ipv6_addr) => is_global_6(ipv6_addr),
        };

        // 3. Early return for global IPs
        if is_global {
            return Ok(());
        }

        // 4. check if the IP is allow-listed
        for cidr in &self.allow_list {
            if cidr.contains(&addr) {
                return Ok(());
            }
        }
        Err(Error::DeniedLocalIp)
    }
}

#[derive(Debug)]
pub enum Error {
    DeniedLocalIp,
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Rejected IP")
    }
}

impl std::error::Error for Error {}

/// See <https://doc.rust-lang.org/src/core/net/ip_addr.rs.html#839>
fn is_global_4(ipv4_addr: std::net::Ipv4Addr) -> bool {
    !(ipv4_addr.octets()[0] == 0 // "This network"
            || ipv4_addr.is_private()
            || ipv4_addr.octets()[0] == 100 && (ipv4_addr.octets()[1] & 0b1100_0000 == 0b0100_0000) // is_shared
            || ipv4_addr.is_loopback()
            || ipv4_addr.is_link_local()
            // addresses reserved for future protocols (`192.0.0.0/24`)
            // .9 and .10 are documented as globally reachable so they're excluded
            || (
                ipv4_addr.octets()[0] == 192 && ipv4_addr.octets()[1] == 0 && ipv4_addr.octets()[2] == 0
                && ipv4_addr.octets()[3] != 9 && ipv4_addr.octets()[3] != 10
            )
            || ipv4_addr.is_documentation()
            || ipv4_addr.octets()[0] == 198 && (ipv4_addr.octets()[1] & 0xfe) == 18 // is_benchmarking
            || ipv4_addr.octets()[0] & 240 == 240 && !ipv4_addr.is_broadcast() // is_reserved
            || ipv4_addr.is_broadcast())
}

/// See <https://doc.rust-lang.org/src/core/net/ip_addr.rs.html#1604>
fn is_global_6(ipv6_addr: std::net::Ipv6Addr) -> bool {
    !(ipv6_addr.is_unspecified()
            || ipv6_addr.is_loopback()
            // IPv4-mapped Address (`::ffff:0:0/96`)
            || matches!(ipv6_addr.segments(), [0, 0, 0, 0, 0, 0xffff, _, _])
            // IPv4-IPv6 Translat. (`64:ff9b:1::/48`)
            || matches!(ipv6_addr.segments(), [0x64, 0xff9b, 1, _, _, _, _, _])
            // Discard-Only Address Block (`100::/64`)
            || matches!(ipv6_addr.segments(), [0x100, 0, 0, 0, _, _, _, _])
            // IETF Protocol Assignments (`2001::/23`)
            || (matches!(ipv6_addr.segments(), [0x2001, b, _, _, _, _, _, _] if b < 0x200)
                && !(
                    // Port Control Protocol Anycast (`2001:1::1`)
                    u128::from_be_bytes(ipv6_addr.octets()) == 0x2001_0001_0000_0000_0000_0000_0000_0001
                    // Traversal Using Relays around NAT Anycast (`2001:1::2`)
                    || u128::from_be_bytes(ipv6_addr.octets()) == 0x2001_0001_0000_0000_0000_0000_0000_0002
                    // AMT (`2001:3::/32`)
                    || matches!(ipv6_addr.segments(), [0x2001, 3, _, _, _, _, _, _])
                    // AS112-v6 (`2001:4:112::/48`)
                    || matches!(ipv6_addr.segments(), [0x2001, 4, 0x112, _, _, _, _, _])
                    // ORCHIDv2 (`2001:20::/28`)
                    // Drone Remote ID Protocol Entity Tags (DETs) Prefix (`2001:30::/28`)`
                    || matches!(ipv6_addr.segments(), [0x2001, b, _, _, _, _, _, _] if b >= 0x20 && b <= 0x3F)
                ))
            // 6to4 (`2002::/16`) – it's not explicitly documented as globally reachable,
            // IANA says N/A.
            || matches!(ipv6_addr.segments(), [0x2002, _, _, _, _, _, _, _])
            || matches!(ipv6_addr.segments(), [0x2001, 0xdb8, ..] | [0x3fff, 0..=0x0fff, ..]) // is_documentation
            // Segment Routing (SRv6) SIDs (`5f00::/16`)
            || matches!(ipv6_addr.segments(), [0x5f00, ..])
            || ipv6_addr.is_unique_local()
            || ipv6_addr.is_unicast_link_local())
}
