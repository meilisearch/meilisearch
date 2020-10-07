use std::net::{IpAddr, SocketAddr};
use std::time::Duration;
use std::collections::HashSet;

use async_stream::stream;
use futures_core::Stream;
use madness::{Packet, MdnsService, META_QUERY_SERVICE};
use madness::dns::{PacketBuilder, ResourceRecord, Class, RData};

/// Returns a stream of dicovered peers
pub fn discover_peers(
    cluster_name: &str,
    id: u64,
    port: u16,
) -> impl Stream<Item = (u64, SocketAddr)> {
    let service_name = format!("_raft_{}._tcp.local", cluster_name);
    stream! {
        let mut known_peers = HashSet::new();
        let mut service = MdnsService::new(false).unwrap();
        let node_service = format!("{}.{}", id, service_name);
        let node_target = format!("{}.local", id);
        service.register(&service_name);
        let _svc_discovery = service.discover(&service_name, Duration::from_secs(5));
        loop {
            let (svc, packet) = service.next().await;
            service = svc;
            match packet {
                Packet::Query(queries) => {
                    for query in queries {
                        if query.is_meta_service_query() {
                            let packet = handle_meta_query(&service_name);
                            service.enqueue_response(packet);
                        } else {
                            match query.name.as_str() {
                                service_name => {
                                    let packet = handle_service_query(
                                        &service_name,
                                        &node_service,
                                        &node_target,
                                        port);
                                    service.enqueue_response(packet);
                                }
                            }
                        }
                    }
                }
                Packet::Response(response) => {
                    let socket_addr = response.socket_address();
                    let id: Option<u64> = response.hostname()
                        .map(|s| s.split(".").next())
                        .flatten()
                        .map(|id| id.parse().ok())
                        .flatten();
                    match (socket_addr, id) {
                        (Some(addr), Some(id)) => {
                            if known_peers.insert(id) {
                                yield (id, addr)
                            }
                        },
                        _ => continue,
                    }
                }
            }
        }
    }
}

fn handle_meta_query(service_name: &str) -> Vec<u8> {
    let mut packet = PacketBuilder::new();
    packet.header_mut()
        .set_id(rand::random())
        .set_query(false);

    packet.add_answer(ResourceRecord::new(
            META_QUERY_SERVICE,
            Duration::from_secs(4500),
            Class::IN,
            RData::ptr(&service_name)));

    packet.build()
}

fn handle_service_query(
    service_name: &str,
    node_service: &str,
    node_target: &str,
    port: u16) -> Vec<u8>
{
    let mut packet = PacketBuilder::new();
    packet.header_mut()
        .set_id(rand::random())
        .set_query(false);

    packet.add_answer(ResourceRecord::new(
            &service_name,
            Duration::from_secs(4500),
            Class::IN,
            RData::ptr(&node_service)));

    packet.add_answer(ResourceRecord::new(
            &service_name,
            Duration::from_secs(4500),
            Class::IN,
            RData::srv(port, 0, 0, &node_target)));

    if let Ok(interfaces) = get_if_addrs::get_if_addrs() {
        for interface in interfaces {
            if interface.is_loopback() {
                continue
            }
            match interface.ip() {
                IpAddr::V4(ip) => {
                    packet.add_answer(ResourceRecord::new(
                            &node_target,
                            Duration::from_secs(4500),
                            Class::IN,
                            RData::a(ip)));
                }
                _ => continue,
            }
        }
    }
    packet.build()
}

