use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use quinn::{Connection, Endpoint, RecvStream, SendStream};
use tokio::sync::{Mutex, RwLock};
use tracing::{debug, info, warn};
use zeroize::Zeroizing;

// Channel type tags sent as handshake byte on each QUIC stream.
const CHANNEL_RAFT: u8 = 0x01;
const CHANNEL_DML: u8 = 0x03;
const CHANNEL_SNAPSHOT: u8 = 0x04;

// Protocol discriminator: first byte sent on the raft channel after
// the connection is established, so the acceptor knows which protocol to expect.
pub(crate) const PROTO_JOIN: u8 = 0x00;
pub(crate) const PROTO_RAFT_RPC: u8 = 0x01;

/// A bidirectional QUIC channel with sequence tracking for replay protection.
///
/// The send+recv pair is held together so an entire RPC cycle
/// (send request → receive response) can be performed atomically under
/// a single Mutex lock, preventing response interleaving.
pub struct ChannelPair {
    pub send: SendStream,
    pub recv: RecvStream,
    pub send_seq: u64,
    pub recv_seq: u64,
}

impl ChannelPair {
    fn new(send: SendStream, recv: RecvStream) -> Self {
        Self { send, recv, send_seq: 1, recv_seq: 0 }
    }
}

/// A peer's 3 bidirectional QUIC channels, each behind a single Mutex
/// to prevent RPC interleaving.
pub struct Peer {
    pub raft: Mutex<ChannelPair>,
    pub dml: Mutex<ChannelPair>,
    pub snapshot: Mutex<ChannelPair>,
}

impl Peer {
    fn new(
        raft: (SendStream, RecvStream),
        dml: (SendStream, RecvStream),
        snapshot: (SendStream, RecvStream),
    ) -> Self {
        Self {
            raft: Mutex::new(ChannelPair::new(raft.0, raft.1)),
            dml: Mutex::new(ChannelPair::new(dml.0, dml.1)),
            snapshot: Mutex::new(ChannelPair::new(snapshot.0, snapshot.1)),
        }
    }
}

/// Result of a `connect_peer()` call, distinguishing new connections from
/// races where another caller already connected the same peer.
pub enum ConnectOutcome {
    /// New connection established — caller should perform handshakes.
    Connected,
    /// Peer was already connected (race lost) — skip handshakes.
    AlreadyConnected,
}

/// QUIC transport layer for the cluster.
///
/// Uses quinn-plaintext (QUIC without TLS encryption) — HMAC signing on every
/// message provides integrity. Each peer connection has 3 multiplexed channels:
/// - **raft**: openraft RPCs (vote, append-entries, install-snapshot metadata)
/// - **dml**: out-of-band file transfers (document uploads)
/// - **snapshot**: initial state seed for node joining
pub struct ClusterTransport {
    endpoint: Endpoint,
    secret: Zeroizing<Vec<u8>>,
    /// Outbound connections (initiated by us).
    peers: RwLock<HashMap<u64, Arc<Peer>>>,
    /// Inbound (accepted) peers with last-activity timestamp for idle cleanup.
    accepted_peers: RwLock<HashMap<u64, Instant>>,
    /// Timeout for accepting all 3 QUIC streams from a peer.
    accept_timeout: Duration,
    /// Last time a Raft RPC succeeded for each peer.
    /// Updated on every successful `rpc_raft` call. Used by the eviction loop
    /// to detect dead followers (stale timestamp = unreachable).
    last_rpc_success: RwLock<HashMap<u64, Arc<std::sync::Mutex<Instant>>>>,
    /// Peers blocked by fault injection (for partition testing).
    /// When a peer ID is in this set, `get_peer()` and `register_peer()` refuse
    /// to operate on it, simulating a network partition at the application level.
    blocked_peers: RwLock<HashSet<u64>>,
    /// Per-node connection lock. Ensures at most one connect_peer() call
    /// is actively establishing a QUIC connection to a given node at a time.
    connecting: RwLock<HashMap<u64, Arc<tokio::sync::Mutex<()>>>>,
    /// Whether to use TLS encryption on QUIC transport.
    tls: bool,
    /// Deterministic self-signed certificate derived from the cluster secret (TLS mode only).
    tls_cert_der: Option<quinn::rustls::pki_types::CertificateDer<'static>>,
    /// Private key for the self-signed certificate (TLS mode only).
    tls_key_der: Option<quinn::rustls::pki_types::PrivateKeyDer<'static>>,
}

impl ClusterTransport {
    /// Create a new QUIC endpoint (server) bound to the given address.
    /// Secret must be at least 16 bytes for HMAC-SHA256 security.
    ///
    /// When `tls` is true, derives a deterministic self-signed certificate from the
    /// cluster secret and uses rustls for encrypted QUIC transport. When false, uses
    /// quinn-plaintext (HMAC-only integrity, no encryption).
    pub async fn new(
        bind_addr: SocketAddr,
        secret: Vec<u8>,
        accept_timeout: Duration,
        tls: bool,
    ) -> Result<Self> {
        anyhow::ensure!(
            secret.len() >= 16,
            "cluster secret must be at least 16 bytes, got {}",
            secret.len()
        );

        // Configure transport with idle timeout and keepalive for dead-peer detection.
        // Without these, QUIC (over UDP) cannot detect a killed peer.
        let mut transport_config = quinn::TransportConfig::default();
        transport_config.max_idle_timeout(Some(
            quinn::IdleTimeout::try_from(Duration::from_secs(15))
                .expect("15s is a valid idle timeout"),
        ));
        transport_config.keep_alive_interval(Some(Duration::from_secs(5)));
        let transport_config = Arc::new(transport_config);

        let (endpoint, tls_cert_der, tls_key_der) = if tls {
            let (cert_der, key_der) = generate_cert_from_secret(&secret)?;
            let server_config = build_tls_server_config(
                &cert_der,
                &key_der,
                transport_config.clone(),
            )?;
            let endpoint = Endpoint::server(server_config, bind_addr)
                .with_context(|| format!("failed to bind QUIC endpoint on {bind_addr}"))?;
            info!(%bind_addr, "Cluster QUIC transport ready (TLS + HMAC signing)");
            (endpoint, Some(cert_der), Some(key_der))
        } else {
            let mut server_config = quinn_plaintext::server_config();
            server_config.transport_config(transport_config.clone());
            let endpoint = Endpoint::server(server_config, bind_addr)
                .with_context(|| format!("failed to bind QUIC endpoint on {bind_addr}"))?;
            info!(%bind_addr, "Cluster QUIC transport ready (plaintext + HMAC signing)");
            (endpoint, None, None)
        };

        Ok(Self {
            endpoint,
            secret: Zeroizing::new(secret),
            peers: RwLock::new(HashMap::new()),
            accepted_peers: RwLock::new(HashMap::new()),
            accept_timeout,
            last_rpc_success: RwLock::new(HashMap::new()),
            blocked_peers: RwLock::new(HashSet::new()),
            connecting: RwLock::new(HashMap::new()),
            tls,
            tls_cert_der,
            tls_key_der,
        })
    }

    /// Get or create a per-node connection mutex to prevent concurrent connect_peer calls.
    async fn node_connect_lock(&self, node_id: u64) -> Arc<tokio::sync::Mutex<()>> {
        // Fast path: read lock
        if let Some(lock) = self.connecting.read().await.get(&node_id) {
            return lock.clone();
        }
        // Slow path: write lock to insert
        self.connecting
            .write()
            .await
            .entry(node_id)
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone()
    }

    /// Get the shared secret for HMAC signing.
    pub fn secret(&self) -> &[u8] {
        &self.secret
    }

    /// Get the local address this endpoint is bound to.
    pub fn local_addr(&self) -> Result<SocketAddr> {
        self.endpoint.local_addr().context("failed to get local address")
    }

    /// Connect to a peer and establish 3 bidirectional channels.
    /// Sends a 1-byte channel type tag on each stream so the acceptor can
    /// identify channels regardless of QUIC stream arrival order.
    ///
    /// Uses a per-node mutex to prevent two concurrent callers from creating
    /// duplicate QUIC connections to the same peer. If the peer is already
    /// connected when the lock is acquired, returns `AlreadyConnected`.
    ///
    /// `proto` is the protocol discriminator sent on the raft channel:
    /// - `PROTO_JOIN` (0x00): joining node handshake
    /// - `PROTO_RAFT_RPC` (0x01): normal Raft RPC session
    pub async fn connect_peer(
        &self,
        node_id: u64,
        addr: SocketAddr,
        proto: u8,
    ) -> Result<ConnectOutcome> {
        if self.blocked_peers.read().await.contains(&node_id) {
            anyhow::bail!("peer {node_id} is blocked (fault injection)");
        }

        // Acquire per-node lock to prevent duplicate concurrent connections.
        let lock = self.node_connect_lock(node_id).await;
        let _guard = lock.lock().await;

        // Re-check after acquiring lock — another caller may have connected.
        if self.peers.read().await.contains_key(&node_id) {
            debug!(node_id, "Peer already connected (race resolved)");
            return Ok(ConnectOutcome::AlreadyConnected);
        }

        debug!(node_id, %addr, proto, "Connecting to peer");

        let client_config = self.build_client_config()?;

        let connecting = self
            .endpoint
            .connect_with(client_config, addr, "meilisearch-cluster")
            .context("failed to create QUIC connection")?;

        // Timeout the QUIC handshake — without this, connecting to a dead node
        // blocks indefinitely (QUIC/UDP has no connection-refused signal).
        let conn = tokio::time::timeout(Duration::from_secs(5), connecting)
            .await
            .map_err(|_| anyhow::anyhow!("QUIC connect to {addr} timed out (5s)"))?
            .with_context(|| format!("failed to connect to peer {node_id} at {addr}"))?;

        let mut raft = open_tagged_bi(&conn, CHANNEL_RAFT).await.context("raft channel")?;
        let dml = open_tagged_bi(&conn, CHANNEL_DML).await.context("dml channel")?;
        let snapshot = open_tagged_bi(&conn, CHANNEL_SNAPSHOT).await.context("snapshot channel")?;

        // Send protocol discriminator on raft channel
        raft.0.write_all(&[proto]).await.context("failed to send protocol discriminator")?;

        info!(node_id, %addr, proto, "Connected to peer (3 channels established)");

        self.peers.write().await.insert(node_id, Arc::new(Peer::new(raft, dml, snapshot)));

        Ok(ConnectOutcome::Connected)
    }

    /// Build a QUIC client config matching the transport mode (TLS or plaintext).
    fn build_client_config(&self) -> Result<quinn::ClientConfig> {
        let mut transport_config = quinn::TransportConfig::default();
        transport_config.max_idle_timeout(Some(
            quinn::IdleTimeout::try_from(Duration::from_secs(15))
                .expect("15s is a valid idle timeout"),
        ));
        transport_config.keep_alive_interval(Some(Duration::from_secs(5)));

        let mut client_config = if self.tls {
            let cert_der = self.tls_cert_der.as_ref().expect("TLS cert must be set in TLS mode");
            let key_der = self.tls_key_der.as_ref().expect("TLS key must be set in TLS mode");
            build_tls_client_config(cert_der, key_der)?
        } else {
            quinn_plaintext::client_config()
        };
        client_config.transport_config(Arc::new(transport_config));
        Ok(client_config)
    }

    /// Accept an incoming peer connection and establish 3 bidirectional channels.
    /// Reads the 1-byte channel tag from each stream to assign channels correctly
    /// regardless of QUIC stream arrival order.
    /// Returns the (peer, protocol) discriminator. The caller should branch on the
    /// protocol: `PROTO_JOIN` for join handshake, `PROTO_RAFT_RPC` for direct RPC.
    pub async fn accept_peer(&self) -> Result<(Arc<Peer>, u8, SocketAddr)> {
        let incoming = self
            .endpoint
            .accept()
            .await
            .context("endpoint closed, no more incoming connections")?;

        let conn = incoming.await.context("failed to accept incoming connection")?;
        let remote_addr = conn.remote_address();

        // Accept 3 tagged streams with a timeout to prevent hanging on partial connections
        let (raft, dml, snapshot, proto) =
            tokio::time::timeout(self.accept_timeout, accept_tagged_streams(&conn))
                .await
                .map_err(|_| anyhow::anyhow!("timed out waiting for peer to open all 3 channels"))?
                .context("failed to accept tagged streams")?;

        let proto_name = match proto {
            PROTO_JOIN => "join",
            PROTO_RAFT_RPC => "raft-rpc",
            _ => "unknown",
        };
        info!(
            remote = %remote_addr,
            proto = proto_name,
            "Accepted peer connection"
        );

        Ok((Arc::new(Peer::new(raft, dml, snapshot)), proto, remote_addr))
    }

    /// Register an accepted peer under a known node ID.
    /// Refuses to register a blocked peer (fault injection).
    pub async fn register_peer(&self, node_id: u64, peer: Arc<Peer>) {
        if self.blocked_peers.read().await.contains(&node_id) {
            warn!(node_id, "Refusing to register blocked peer (fault injection)");
            return;
        }
        info!(node_id, "Registered peer");
        self.peers.write().await.insert(node_id, peer);
    }

    /// Get a peer by ID (clones the Arc).
    /// Returns an error if the peer is blocked (fault injection).
    pub async fn get_peer(&self, peer_id: u64) -> Result<Arc<Peer>> {
        if self.blocked_peers.read().await.contains(&peer_id) {
            anyhow::bail!("peer {peer_id} is blocked (fault injection)");
        }
        let peers = self.peers.read().await;
        peers.get(&peer_id).cloned().with_context(|| format!("peer {peer_id} not connected"))
    }

    /// Perform an atomic RPC on the raft channel: send request, receive response.
    /// Holds a single Mutex lock for the entire cycle to prevent interleaving
    /// when openraft makes concurrent RPCs to the same peer.
    pub async fn rpc_raft(&self, peer_id: u64, data: &[u8]) -> Result<Vec<u8>> {
        let peer = self.get_peer(peer_id).await?;
        let ch = &mut *peer.raft.lock().await;

        // Send with monotonic sequence number
        let seq = ch.send_seq;
        ch.send_seq += 1;
        crate::framing::send_signed(&mut ch.send, seq, data, &self.secret).await?;

        // Receive and verify monotonic sequence
        let (recv_seq, resp_data) = crate::framing::recv_signed(&mut ch.recv, &self.secret).await?;
        if recv_seq <= ch.recv_seq {
            anyhow::bail!(
                "replay detected on raft channel: received seq {recv_seq}, expected > {}",
                ch.recv_seq
            );
        }
        ch.recv_seq = recv_seq;

        Ok(resp_data)
    }

    /// Send a DML header + stream file chunks, then receive the ACK.
    /// This streams from disk in 64KB chunks to avoid loading the entire file into memory.
    pub async fn rpc_dml_stream_file(
        &self,
        peer_id: u64,
        header: &crate::rpc_handler::DmlHeader,
        file_path: &std::path::Path,
    ) -> Result<Vec<u8>> {
        let peer = self.get_peer(peer_id).await?;
        let ch = &mut *peer.dml.lock().await;

        // Send header
        let header_data = bincode::serialize(header).context("failed to serialize DML header")?;
        let seq = ch.send_seq;
        ch.send_seq += 1;
        crate::framing::send_signed(&mut ch.send, seq, &header_data, &self.secret).await?;

        // Stream file in chunks using BufReader to avoid loading the entire file into memory
        let file = tokio::task::block_in_place(|| {
            std::fs::File::open(file_path)
                .with_context(|| format!("failed to open file {}", file_path.display()))
        })?;
        let mut reader =
            std::io::BufReader::with_capacity(crate::rpc_handler::DML_CHUNK_SIZE, file);
        loop {
            let buf = tokio::task::block_in_place(|| {
                use std::io::Read;
                let mut buf = vec![0u8; crate::rpc_handler::DML_CHUNK_SIZE];
                let n = reader.read(&mut buf)?;
                buf.truncate(n);
                Ok::<_, std::io::Error>(buf)
            })?;
            if buf.is_empty() {
                break;
            }
            let seq = ch.send_seq;
            ch.send_seq += 1;
            crate::framing::send_signed(&mut ch.send, seq, &buf, &self.secret).await?;
        }

        // Receive ACK
        let (recv_seq, resp_data) = crate::framing::recv_signed(&mut ch.recv, &self.secret).await?;
        if recv_seq <= ch.recv_seq {
            anyhow::bail!(
                "replay detected on DML channel: received seq {recv_seq}, expected > {}",
                ch.recv_seq
            );
        }
        ch.recv_seq = recv_seq;

        Ok(resp_data)
    }

    /// Request a content file from a peer via the snapshot channel.
    ///
    /// Sends a `FileFetchRequest` and receives the file if found.
    /// The snapshot channel is repurposed for file serving after the initial join.
    pub async fn fetch_file_from_peer(
        &self,
        peer_id: u64,
        uuid_str: &str,
        dest_path: &std::path::Path,
    ) -> Result<()> {
        let peer = self.get_peer(peer_id).await?;

        // Send request on snapshot channel
        {
            let ch = &mut *peer.snapshot.lock().await;
            let req = crate::rpc_handler::FileFetchRequest { uuid_str: uuid_str.to_string() };
            let data = bincode::serialize(&req).context("failed to serialize file fetch request")?;
            let seq = ch.send_seq;
            ch.send_seq += 1;
            crate::framing::send_signed(&mut ch.send, seq, &data, &self.secret).await?;
        }

        // Read response
        let response = {
            let ch = &mut *peer.snapshot.lock().await;
            let (seq, data) = crate::framing::recv_signed(&mut ch.recv, &self.secret).await?;
            if seq <= ch.recv_seq {
                anyhow::bail!(
                    "replay detected on file-fetch response: seq {seq}, expected > {}",
                    ch.recv_seq
                );
            }
            ch.recv_seq = seq;
            bincode::deserialize::<crate::rpc_handler::FileFetchResponse>(&data)
                .context("failed to deserialize file fetch response")?
        };

        match response {
            crate::rpc_handler::FileFetchResponse::Found { size } => {
                // Receive file chunks
                let total = size as usize;
                let mut received = 0;
                let mut tmp = tokio::task::block_in_place(|| {
                    tempfile::NamedTempFile::new_in(
                        dest_path
                            .parent()
                            .unwrap_or(std::path::Path::new(".")),
                    )
                })
                .context("failed to create tempfile for fetched content")?;

                while received < total {
                    let ch = &mut *peer.snapshot.lock().await;
                    let (seq, chunk) =
                        crate::framing::recv_signed(&mut ch.recv, &self.secret).await?;
                    if seq <= ch.recv_seq {
                        anyhow::bail!(
                            "replay detected on file-fetch chunk: seq {seq}, expected > {}",
                            ch.recv_seq
                        );
                    }
                    ch.recv_seq = seq;
                    received += chunk.len();

                    tokio::task::block_in_place(|| {
                        use std::io::Write;
                        tmp.write_all(&chunk)
                    })
                    .context("failed to write fetched chunk")?;
                }

                // Finalize
                tokio::task::block_in_place(|| {
                    tmp.as_file()
                        .sync_all()
                        .context("failed to fsync fetched file")?;
                    tmp.persist(dest_path)
                        .context("failed to persist fetched file")?;
                    Ok::<_, anyhow::Error>(())
                })?;

                // Send ACK
                {
                    let ch = &mut *peer.snapshot.lock().await;
                    let ack = bincode::serialize(&crate::rpc_handler::DmlResponse::FileTransferAck)
                        .context("failed to serialize fetch ACK")?;
                    let seq = ch.send_seq;
                    ch.send_seq += 1;
                    crate::framing::send_signed(&mut ch.send, seq, &ack, &self.secret).await?;
                }

                Ok(())
            }
            crate::rpc_handler::FileFetchResponse::NotFound => {
                anyhow::bail!("content file {uuid_str} not found on peer {peer_id}")
            }
        }
    }

    /// Check if a peer is connected.
    pub async fn has_peer(&self, peer_id: u64) -> bool {
        self.peers.read().await.contains_key(&peer_id)
    }

    /// Remove a peer by ID (e.g., cleanup of temporary peer 0 after join handshake).
    pub async fn remove_peer(&self, peer_id: u64) {
        self.peers.write().await.remove(&peer_id);
    }

    /// Get list of connected peer IDs.
    pub async fn peer_ids(&self) -> Vec<u64> {
        self.peers.read().await.keys().copied().collect()
    }

    /// Register an accepted (inbound) peer with current timestamp.
    pub async fn register_accepted_peer(&self, node_id: u64) {
        self.accepted_peers.write().await.insert(node_id, Instant::now());
    }

    /// Update the last-activity timestamp for an accepted peer.
    pub async fn touch_accepted_peer(&self, node_id: u64) {
        if let Some(ts) = self.accepted_peers.write().await.get_mut(&node_id) {
            *ts = Instant::now();
        }
    }

    /// Remove an accepted peer (e.g., on connection close).
    pub async fn remove_accepted_peer(&self, node_id: u64) {
        self.accepted_peers.write().await.remove(&node_id);
    }

    /// Get the idle duration for an accepted peer (time since last activity).
    /// Returns `None` if the peer is not in the accepted peers map.
    pub async fn accepted_peer_idle(&self, node_id: u64) -> Option<Duration> {
        let accepted = self.accepted_peers.read().await;
        accepted.get(&node_id).map(|ts| Instant::now().duration_since(*ts))
    }

    /// Remove accepted peers idle for longer than the given duration.
    /// Returns the IDs of removed peers.
    pub async fn cleanup_idle_accepted_peers(&self, max_idle: Duration) -> Vec<u64> {
        let now = Instant::now();
        let mut removed = Vec::new();
        let mut accepted = self.accepted_peers.write().await;
        accepted.retain(|&id, ts| {
            let idle = now.duration_since(*ts);
            if idle > max_idle {
                warn!(node_id = id, idle_secs = idle.as_secs(), "Removing idle accepted peer");
                removed.push(id);
                false
            } else {
                true
            }
        });
        removed
    }

    /// Record a successful Raft RPC to a peer (updates last-success timestamp).
    /// Called on every successful `append_entries` / `vote` / `install_snapshot` RPC.
    pub async fn touch_rpc_success(&self, node_id: u64) {
        let map = self.last_rpc_success.read().await;
        if let Some(ts) = map.get(&node_id) {
            *ts.lock().unwrap() = Instant::now();
            return;
        }
        drop(map);
        // First success for this peer — create the entry.
        self.last_rpc_success
            .write()
            .await
            .insert(node_id, Arc::new(std::sync::Mutex::new(Instant::now())));
    }

    /// Get the last time a Raft RPC succeeded for a peer.
    /// Returns `None` if no successful RPC has been recorded yet.
    pub async fn rpc_last_success(&self, node_id: u64) -> Option<Instant> {
        let map = self.last_rpc_success.read().await;
        map.get(&node_id).map(|ts| *ts.lock().unwrap())
    }

    // -- Fault injection for partition testing --

    /// Block a peer: all outbound traffic to this peer will fail, and
    /// the existing connection is removed to force a clean break.
    pub async fn block_peer(&self, peer_id: u64) {
        info!(peer_id, "Blocking peer (fault injection)");
        self.blocked_peers.write().await.insert(peer_id);
        // Close existing connection so in-flight RPCs fail immediately.
        self.remove_peer(peer_id).await;
    }

    /// Unblock a previously blocked peer, allowing reconnection.
    pub async fn unblock_peer(&self, peer_id: u64) {
        info!(peer_id, "Unblocking peer (fault injection)");
        self.blocked_peers.write().await.remove(&peer_id);
    }

    /// Return the list of currently blocked peer IDs.
    pub async fn blocked_peers_list(&self) -> Vec<u64> {
        self.blocked_peers.read().await.iter().copied().collect()
    }

    /// Check if a peer is blocked (fault injection).
    pub async fn is_blocked(&self, peer_id: u64) -> bool {
        self.blocked_peers.read().await.contains(&peer_id)
    }

    /// Gracefully shut down the QUIC endpoint.
    /// Closes all peer connections and stops accepting new ones.
    /// The accept loop will see "endpoint closed" and exit.
    pub fn shutdown(&self) {
        info!("Shutting down QUIC transport");
        self.endpoint.close(0u32.into(), b"shutdown");
    }
}

// -- TLS certificate generation from cluster secret --

/// Generate a deterministic self-signed certificate from the cluster secret.
/// All nodes with the same secret produce the same cert+key, so they mutually
/// trust each other without any cert distribution.
fn generate_cert_from_secret(
    secret: &[u8],
) -> Result<(
    quinn::rustls::pki_types::CertificateDer<'static>,
    quinn::rustls::pki_types::PrivateKeyDer<'static>,
)> {
    use sha2::{Digest, Sha256};

    // Derive a deterministic 32-byte Ed25519 seed from the cluster secret.
    let seed: [u8; 32] = Sha256::digest([b"cluster-tls-seed" as &[u8], secret].concat()).into();

    // Construct Ed25519 PKCS8 v2 DER encoding from the seed.
    // Format: SEQUENCE { version, algorithm, privateKey, publicKey }
    // Ed25519 PKCS8 is a fixed-size structure — we build it manually so we
    // can use the deterministic seed without depending on ring's random key generation.
    //
    // The PKCS8 DER format for Ed25519:
    //   SEQUENCE {
    //     INTEGER 0                     -- version
    //     SEQUENCE { OID 1.3.101.112 }  -- Ed25519 algorithm
    //     OCTET STRING {                -- privateKey
    //       OCTET STRING (32 bytes)     -- raw seed
    //     }
    //   }
    let pkcs8_prefix: &[u8] = &[
        0x30, 0x2e, // SEQUENCE (46 bytes)
        0x02, 0x01, 0x00, // INTEGER 0 (version)
        0x30, 0x05, // SEQUENCE (5 bytes)
        0x06, 0x03, 0x2b, 0x65, 0x70, // OID 1.3.101.112 (Ed25519)
        0x04, 0x22, // OCTET STRING (34 bytes)
        0x04, 0x20, // OCTET STRING (32 bytes) -- the seed
    ];
    let mut pkcs8_der = Vec::with_capacity(pkcs8_prefix.len() + 32);
    pkcs8_der.extend_from_slice(pkcs8_prefix);
    pkcs8_der.extend_from_slice(&seed);

    let key_pair = rcgen::KeyPair::from_pkcs8_der_and_sign_algo(
        &quinn::rustls::pki_types::PrivatePkcs8KeyDer::from(pkcs8_der.clone()),
        &rcgen::PKCS_ED25519,
    )
    .context("failed to create Ed25519 key pair from derived seed")?;

    let mut params = rcgen::CertificateParams::new(vec!["meilisearch-cluster".to_string()])
        .context("failed to create certificate params")?;
    // Set a long validity so operators don't need to worry about expiry.
    // Security comes from the shared secret, not cert expiry.
    params.not_before = rcgen::date_time_ymd(2024, 1, 1);
    params.not_after = rcgen::date_time_ymd(2074, 1, 1);

    let cert = params
        .self_signed(&key_pair)
        .context("failed to generate self-signed certificate")?;
    let cert_der = cert.der().clone();
    let key_der = quinn::rustls::pki_types::PrivateKeyDer::Pkcs8(
        quinn::rustls::pki_types::PrivatePkcs8KeyDer::from(key_pair.serialize_der()),
    );
    Ok((cert_der, key_der))
}

/// Custom certificate verifier that accepts only the exact certificate
/// derived from the cluster secret. This is stronger than standard TLS:
/// it proves the peer possesses the cluster secret.
#[derive(Debug)]
struct ClusterCertVerifier {
    expected_cert: quinn::rustls::pki_types::CertificateDer<'static>,
}

impl quinn::rustls::client::danger::ServerCertVerifier for ClusterCertVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &quinn::rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[quinn::rustls::pki_types::CertificateDer<'_>],
        _server_name: &quinn::rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: quinn::rustls::pki_types::UnixTime,
    ) -> std::result::Result<quinn::rustls::client::danger::ServerCertVerified, quinn::rustls::Error>
    {
        if end_entity.as_ref() == self.expected_cert.as_ref() {
            Ok(quinn::rustls::client::danger::ServerCertVerified::assertion())
        } else {
            Err(quinn::rustls::Error::General(
                "peer certificate does not match cluster secret".to_string(),
            ))
        }
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &quinn::rustls::pki_types::CertificateDer<'_>,
        _dss: &quinn::rustls::DigitallySignedStruct,
    ) -> std::result::Result<quinn::rustls::client::danger::HandshakeSignatureValid, quinn::rustls::Error>
    {
        // TLS 1.2 is not used with QUIC
        Err(quinn::rustls::Error::General("TLS 1.2 not supported".to_string()))
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &quinn::rustls::pki_types::CertificateDer<'_>,
        dss: &quinn::rustls::DigitallySignedStruct,
    ) -> std::result::Result<quinn::rustls::client::danger::HandshakeSignatureValid, quinn::rustls::Error>
    {
        quinn::rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &quinn::rustls::crypto::ring::default_provider().signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<quinn::rustls::SignatureScheme> {
        quinn::rustls::crypto::ring::default_provider()
            .signature_verification_algorithms
            .supported_schemes()
    }
}

impl quinn::rustls::server::danger::ClientCertVerifier for ClusterCertVerifier {
    fn root_hint_subjects(&self) -> &[quinn::rustls::DistinguishedName] {
        &[]
    }

    fn verify_client_cert(
        &self,
        end_entity: &quinn::rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[quinn::rustls::pki_types::CertificateDer<'_>],
        _now: quinn::rustls::pki_types::UnixTime,
    ) -> std::result::Result<quinn::rustls::server::danger::ClientCertVerified, quinn::rustls::Error>
    {
        if end_entity.as_ref() == self.expected_cert.as_ref() {
            Ok(quinn::rustls::server::danger::ClientCertVerified::assertion())
        } else {
            Err(quinn::rustls::Error::General(
                "peer certificate does not match cluster secret".to_string(),
            ))
        }
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &quinn::rustls::pki_types::CertificateDer<'_>,
        _dss: &quinn::rustls::DigitallySignedStruct,
    ) -> std::result::Result<quinn::rustls::client::danger::HandshakeSignatureValid, quinn::rustls::Error>
    {
        Err(quinn::rustls::Error::General("TLS 1.2 not supported".to_string()))
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &quinn::rustls::pki_types::CertificateDer<'_>,
        dss: &quinn::rustls::DigitallySignedStruct,
    ) -> std::result::Result<quinn::rustls::client::danger::HandshakeSignatureValid, quinn::rustls::Error>
    {
        quinn::rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &quinn::rustls::crypto::ring::default_provider().signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<quinn::rustls::SignatureScheme> {
        quinn::rustls::crypto::ring::default_provider()
            .signature_verification_algorithms
            .supported_schemes()
    }

    fn offer_client_auth(&self) -> bool {
        true
    }

    fn client_auth_mandatory(&self) -> bool {
        true
    }
}

/// Build a rustls-based QUIC server config with mutual TLS using the cluster cert.
fn build_tls_server_config(
    cert_der: &quinn::rustls::pki_types::CertificateDer<'static>,
    key_der: &quinn::rustls::pki_types::PrivateKeyDer<'static>,
    transport_config: Arc<quinn::TransportConfig>,
) -> Result<quinn::ServerConfig> {
    let verifier = Arc::new(ClusterCertVerifier { expected_cert: cert_der.clone() });
    let mut tls_config = quinn::rustls::ServerConfig::builder_with_provider(Arc::new(
        quinn::rustls::crypto::ring::default_provider(),
    ))
    .with_protocol_versions(&[&quinn::rustls::version::TLS13])
    .context("failed to set TLS 1.3 protocol version")?
    .with_client_cert_verifier(verifier)
    .with_single_cert(vec![cert_der.clone()], key_der.clone_key())
    .context("failed to configure server TLS certificate")?;
    tls_config.alpn_protocols = vec![b"meilisearch-cluster".to_vec()];
    let mut server_config = quinn::ServerConfig::with_crypto(Arc::new(
        quinn::crypto::rustls::QuicServerConfig::try_from(tls_config)
            .context("failed to create QUIC server config from TLS config")?,
    ));
    server_config.transport_config(transport_config);
    Ok(server_config)
}

/// Build a rustls-based QUIC client config with mutual TLS using the cluster cert.
fn build_tls_client_config(
    cert_der: &quinn::rustls::pki_types::CertificateDer<'static>,
    key_der: &quinn::rustls::pki_types::PrivateKeyDer<'static>,
) -> Result<quinn::ClientConfig> {
    let verifier = Arc::new(ClusterCertVerifier { expected_cert: cert_der.clone() });
    let mut tls_config = quinn::rustls::ClientConfig::builder_with_provider(Arc::new(
        quinn::rustls::crypto::ring::default_provider(),
    ))
    .with_protocol_versions(&[&quinn::rustls::version::TLS13])
    .context("failed to set TLS 1.3 protocol version")?
    .dangerous()
    .with_custom_certificate_verifier(verifier)
    .with_client_auth_cert(vec![cert_der.clone()], key_der.clone_key())
    .context("failed to configure client TLS certificate")?;
    tls_config.alpn_protocols = vec![b"meilisearch-cluster".to_vec()];
    Ok(quinn::ClientConfig::new(Arc::new(
        quinn::crypto::rustls::QuicClientConfig::try_from(tls_config)
            .context("failed to create QUIC client config from TLS config")?,
    )))
}

/// Open a bidirectional QUIC stream and send a 1-byte channel tag.
async fn open_tagged_bi(conn: &Connection, tag: u8) -> Result<(SendStream, RecvStream)> {
    let (mut send, recv) = conn.open_bi().await.context("failed to open stream")?;
    send.write_all(&[tag]).await.context("failed to send channel tag")?;
    Ok((send, recv))
}

/// Accept 3 tagged bidirectional streams and a protocol discriminator from a connection.
/// Returns (raft, dml, snapshot, proto).
/// Detects duplicate channel tags and unknown tags.
async fn accept_tagged_streams(
    conn: &Connection,
) -> Result<(
    (SendStream, RecvStream),
    (SendStream, RecvStream),
    (SendStream, RecvStream),
    u8,
)> {
    let mut raft = None;
    let mut dml = None;
    let mut snapshot = None;

    for _ in 0..3 {
        let (send, mut recv) = conn.accept_bi().await.context("failed to accept stream")?;
        let mut tag = [0u8; 1];
        recv.read_exact(&mut tag).await.context("failed to read channel tag")?;

        match tag[0] {
            CHANNEL_RAFT if raft.is_none() => raft = Some((send, recv)),
            CHANNEL_DML if dml.is_none() => dml = Some((send, recv)),
            CHANNEL_SNAPSHOT if snapshot.is_none() => snapshot = Some((send, recv)),
            CHANNEL_RAFT | CHANNEL_DML | CHANNEL_SNAPSHOT => {
                anyhow::bail!("duplicate channel tag: 0x{:02x}", tag[0]);
            }
            other => anyhow::bail!("unknown channel tag: 0x{other:02x}"),
        }
    }

    let mut raft = raft.context("peer did not open raft channel")?;
    let dml = dml.context("peer did not open dml channel")?;
    let snapshot = snapshot.context("peer did not open snapshot channel")?;

    // Read protocol discriminator from raft channel
    let mut proto_buf = [0u8; 1];
    raft.1.read_exact(&mut proto_buf).await.context("failed to read protocol discriminator")?;

    Ok((raft, dml, snapshot, proto_buf[0]))
}
