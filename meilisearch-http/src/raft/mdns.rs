use std::collections::HashSet;
use std::net::SocketAddr;
use std::time::Duration;

use anyhow::Result;
use futures_util::{pin_mut, stream::StreamExt};
use libmdns::Responder;
use mdns::discover;
use tokio::sync::{broadcast, mpsc};

const RAFT_SERVICE: &str = "_meili-raft._tcp";

#[derive(Debug, Clone)]
pub struct Node {
    pub addr: SocketAddr,
    pub id: u64,
}

#[derive(Debug)]
struct Ad {
    port: u16,
    id: u64,
}

struct Server {
    rx: mpsc::Receiver<Ad>,
}

impl Server {
    fn run(mut self) -> Result<()> {
        let (responder, task) = Responder::with_default_handle()?;
        tokio::spawn(task);
        tokio::spawn(async move {
            let mut services = Vec::new();
            loop {
                match self.rx.recv().await {
                    Some(Ad { port, id, .. }) => {
                        let svc = responder.register(
                            RAFT_SERVICE.to_owned(),
                            RAFT_SERVICE.to_owned(),
                            port,
                            &[&format!("id:{}", id)],
                        );
                        services.push(svc);
                    }
                    _ => (),
                }
            }
        });
        Ok(())
    }
}

struct Client {
    tx: broadcast::Sender<Node>,
    known_hosts: HashSet<String>,
}

impl Client {
    async fn run(mut self, discovery_interval: Duration) -> Result<()> {
        let stream =
            discover::all(&format!("{}.local", RAFT_SERVICE), discovery_interval)?.listen();
        pin_mut!(stream);
        loop {
            match stream.next().await {
                Some(Ok(response)) => {
                    if let Some(addr) = response.socket_address() {
                        if self.known_hosts.insert(addr.to_string()) {
                            println!("new host! addr: {}", addr.to_string());
                            let id = response
                                .txt_records()
                                .filter_map(|r| {
                                    let mut split = r.split(":");
                                    match (split.next(), split.next()) {
                                        (Some(key), Some(value)) if key == "id" => {
                                            value.parse().ok()
                                        }
                                        _ => None,
                                    }
                                })
                                .next();
                            if let Some(id) = id {
                                let _ = self.tx.send(Node { id, addr });
                            }
                        }
                    }
                }
                _ => (),
            }
        }
    }
}

pub struct MDNSServer {
    broadcast_tx: broadcast::Sender<Node>,
    server_tx: mpsc::Sender<Ad>,
}

impl MDNSServer {
    pub fn new(discover_duration: Duration) -> Result<MDNSServer> {
        let (broadcast_tx, _) = broadcast::channel(1000);
        let client = Client {
            tx: broadcast_tx.clone(),
            known_hosts: HashSet::new(),
        };
        let _client_handle = tokio::spawn(client.run(discover_duration));
        let (server_tx, rx) = mpsc::channel(1000);
        let server = Server { rx };
        let _server_handle = server.run()?;
        Ok(MDNSServer {
            broadcast_tx,
            server_tx,
        })
    }

    /// returns a receiver to discovered nodes.
    pub fn discover(&self) -> broadcast::Receiver<Node> {
        self.broadcast_tx.subscribe()
    }

    pub async fn advertise(&mut self, id: u64, port: u16) -> Result<()> {
        self.server_tx.send(Ad { id, port }).await?;
        Ok(())
    }
}
