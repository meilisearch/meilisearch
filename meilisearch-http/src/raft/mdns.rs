use std::net::{IpAddr, SocketAddr};
use std::time::Duration;

use async_stream::stream;
use futures_core::Stream;
use madness::dns::PacketBuilder;
use madness::packet::MdnsPacket;
use madness::service::MdnsService;

/// Returns a stream of dicovered peers
pub fn discover_peers(
    service_name: &str,
    id: u64,
    port: u16,
) -> impl Stream<Item = (u64, SocketAddr)> {
    let service_name = service_name.to_owned();
    stream! {
        let mut service = MdnsService::new(false).unwrap();
        let node_fqdn = format!("{}.{}", id, service_name);
        let node_target = format!("{}.local", id);
        service.register(&service_name);
        let _svc_discovery = service.discover(&service_name, Duration::from_secs(5));
        loop {
            let (mut srv, packets) = service.next().await;
            for packet in packets {
                match packet {
                    MdnsPacket::Query(query) => {
                        if query.service_name == service_name {
                            let mut resp = PacketBuilder::new();
                            resp.add_ptr(&service_name, &node_fqdn, Duration::from_secs(3600));
                            resp.add_srv(&service_name, port, Duration::from_secs(3600), 1, 1, &node_target);

                            // add an address record with the IpAddr for each interface
                            // TODO: this can probably just be done once
                            if let Ok(interfaces) = get_if_addrs::get_if_addrs() {
                                for interface in interfaces {
                                    if interface.is_loopback() {
                                        continue
                                    }
                                    match interface.ip() {
                                        IpAddr::V4(ip) => {
                                            resp.add_a(&node_target, ip, Duration::from_secs(3600));
                                        }
                                        _ => continue,
                                    }
                                }
                            }

                            let resp = resp.build_answer(rand::random());
                            srv.enqueue_response(resp);
                        }
                    }
                    MdnsPacket::Response(response) => {
                        let socket_addr = response.socket_address();
                        let id: Option<u64> = response.hostname()
                            .map(|s| s.split(".").next())
                            .flatten()
                            .map(|id| id.parse().ok())
                            .flatten();
                        match (socket_addr, id) {
                            (Some(addr), Some(id)) => {
                                yield (id, addr)
                            },
                            _ => continue,
                        }
                    }
                    MdnsPacket::ServiceDiscovery(_disc) => {
                        let mut packet = PacketBuilder::new();
                        packet.add_ptr("_services._dns-sd._udp.local", &service_name, Duration::from_secs(3600));
                        let packet = packet.build_answer(rand::random());
                        srv.enqueue_response(packet);
                    }
                }
            }
            service = srv;
        }
    }
}
