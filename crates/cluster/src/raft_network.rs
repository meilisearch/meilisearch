use std::net::SocketAddr;
use std::sync::Arc;

use openraft::error::{InstallSnapshotError, NetworkError, RPCError, RaftError, Unreachable};
use openraft::network::RPCOption;
use openraft::network::{RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::BasicNode;
use tracing::{debug, info, warn};

use crate::rpc_handler::{RaftRpc, RaftRpcResponse};
use crate::transport::ClusterTransport;
use crate::types::TypeConfig;

/// Factory that creates per-target network connections for openraft.
pub struct QuinnNetworkFactory {
    pub transport: Arc<ClusterTransport>,
    pub our_node_id: u64,
}

impl RaftNetworkFactory<TypeConfig> for QuinnNetworkFactory {
    type Network = QuinnPeerNetwork;

    async fn new_client(&mut self, target: u64, node: &BasicNode) -> Self::Network {
        let target_quic_addr = crate::decode_node_quic_addr(&node.addr);
        QuinnPeerNetwork {
            transport: self.transport.clone(),
            target,
            our_node_id: self.our_node_id,
            target_quic_addr,
        }
    }
}

/// Per-peer network implementation for openraft RPCs over QUIC.
///
/// openraft calls these methods to send Raft RPCs (vote, append-entries, snapshot)
/// to a specific peer. We wrap each request in a `RaftRpc` tagged envelope,
/// send it over the signed QUIC raft channel, and unwrap the `RaftRpcResponse`.
///
/// If the peer isn't connected yet, the network auto-connects using the QUIC
/// address extracted from `BasicNode.addr` (combined `quic|http` format).
pub struct QuinnPeerNetwork {
    transport: Arc<ClusterTransport>,
    target: u64,
    our_node_id: u64,
    target_quic_addr: Option<SocketAddr>,
}

/// Error type for network operations that implements std::error::Error
/// (unlike anyhow::Error which doesn't).
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
struct TransportError(String);

impl QuinnPeerNetwork {
    /// Send a tagged RPC envelope and receive the response.
    /// Auto-connects to the peer if not already connected.
    /// On connection error, removes the stale peer and retries once.
    async fn send_rpc<E: std::error::Error>(
        &self,
        rpc: &RaftRpc,
    ) -> Result<RaftRpcResponse, RPCError<u64, BasicNode, E>> {
        let data =
            bincode::serialize(rpc).map_err(|e| RPCError::Network(NetworkError::new(&*e)))?;

        // Try up to 2 times: first attempt may hit a stale peer, second auto-reconnects.
        for attempt in 0..2 {
            // Auto-connect if the peer isn't registered in the transport yet
            if !self.transport.has_peer(self.target).await {
                self.auto_connect().await?;
            }

            match self.transport.rpc_raft(self.target, &data).await {
                Ok(resp_data) => {
                    self.transport.touch_rpc_success(self.target).await;
                    return bincode::deserialize(&resp_data)
                        .map_err(|e| RPCError::Network(NetworkError::new(&*e)));
                }
                Err(e) => {
                    if attempt == 0 {
                        // First failure: remove stale peer so retry triggers auto-connect.
                        warn!(
                            target_node = self.target,
                            error = %e,
                            "raft RPC failed, removing stale peer for reconnect"
                        );
                        self.transport.remove_peer(self.target).await;
                    } else {
                        warn!(target_node = self.target, error = %e, "raft RPC failed after reconnect");
                        return Err(RPCError::Unreachable(Unreachable::new(&TransportError(
                            e.to_string(),
                        ))));
                    }
                }
            }
        }

        unreachable!("loop always returns")
    }

    /// Establish an outbound QUIC connection to the target peer.
    /// Sends the PROTO_RAFT_RPC discriminator and our node ID so the
    /// target's accept loop can register us and spawn an RPC handler.
    ///
    /// If another caller already connected this peer (race resolved by per-node
    /// mutex in `connect_peer`), skips handshakes and returns immediately.
    async fn auto_connect<E: std::error::Error>(&self) -> Result<(), RPCError<u64, BasicNode, E>> {
        let quic_addr = self.target_quic_addr.ok_or_else(|| {
            RPCError::Unreachable(Unreachable::new(&TransportError(format!(
                "peer {} not connected and no QUIC address available",
                self.target
            ))))
        })?;

        info!(
            target = self.target,
            %quic_addr,
            "Auto-connecting to peer for Raft RPCs"
        );

        // Connect and open 3 tagged QUIC channels
        let outcome = self
            .transport
            .connect_peer(self.target, quic_addr, crate::transport::PROTO_RAFT_RPC)
            .await
            .map_err(|e| {
                warn!(
                    target_node = self.target,
                    %quic_addr,
                    error = %e,
                    "Auto-connect to peer failed"
                );
                RPCError::Unreachable(Unreachable::new(&TransportError(e.to_string())))
            })?;

        // If another caller already connected, skip handshakes
        if matches!(outcome, crate::transport::ConnectOutcome::AlreadyConnected) {
            debug!(target = self.target, "Peer already connected, skipping handshakes");
            return Ok(());
        }

        // Send PeerHandshake on the raft channel so the acceptor can identify us
        // and store our version/protocol info.
        let peer =
            self.transport.get_peer(self.target).await.map_err(|e| {
                RPCError::Unreachable(Unreachable::new(&TransportError(e.to_string())))
            })?;
        {
            let handshake = crate::messages::PeerHandshake {
                node_id: self.our_node_id,
                binary_version: env!("CARGO_PKG_VERSION").to_string(),
                supported_protocols: crate::SUPPORTED_PROTOCOLS.to_vec(),
            };
            let data = bincode::serialize(&handshake).map_err(|e| {
                RPCError::Network(NetworkError::new(&*e))
            })?;
            let ch = &mut *peer.raft.lock().await;
            let seq = ch.send_seq;
            ch.send_seq += 1;
            crate::framing::send_signed(
                &mut ch.send,
                seq,
                &data,
                self.transport.secret(),
            )
            .await
            .map_err(|e| {
                RPCError::Unreachable(Unreachable::new(&TransportError(e.to_string())))
            })?;
        }

        // Perform DML channel handshake (connector side) with timeout
        {
            let ch = &mut *peer.dml.lock().await;
            tokio::time::timeout(
                std::time::Duration::from_secs(5),
                crate::framing::dml_handshake_connector(ch, self.transport.secret()),
            )
            .await
            .map_err(|_| {
                RPCError::Unreachable(Unreachable::new(&TransportError(format!(
                    "DML handshake timed out connecting to peer {}",
                    self.target
                ))))
            })?
            .map_err(|e| {
                RPCError::Unreachable(Unreachable::new(&TransportError(format!(
                    "DML handshake failed: {e}"
                ))))
            })?;
        }

        info!(target = self.target, "Auto-connected to peer");

        Ok(())
    }
}

impl RaftNetwork<TypeConfig> for QuinnPeerNetwork {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        debug!(target_node = self.target, "sending append_entries");
        let envelope = RaftRpc::AppendEntries(rpc);
        match self.send_rpc(&envelope).await? {
            RaftRpcResponse::AppendEntries(r) => r.map_err(|e| {
                RPCError::RemoteError(openraft::error::RemoteError::new(self.target, e))
            }),
            other => Err(RPCError::Network(NetworkError::new(&TransportError(format!(
                "unexpected response variant: expected AppendEntries, got {other:?}"
            ))))),
        }
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<u64>,
        RPCError<u64, BasicNode, RaftError<u64, InstallSnapshotError>>,
    > {
        debug!(target_node = self.target, "sending install_snapshot");
        let envelope = RaftRpc::InstallSnapshot(rpc);
        match self.send_rpc(&envelope).await? {
            RaftRpcResponse::InstallSnapshot(r) => r.map_err(|e| {
                RPCError::RemoteError(openraft::error::RemoteError::new(self.target, e))
            }),
            other => Err(RPCError::Network(NetworkError::new(&TransportError(format!(
                "unexpected response variant: expected InstallSnapshot, got {other:?}"
            ))))),
        }
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<u64>,
        _option: RPCOption,
    ) -> Result<VoteResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        debug!(target_node = self.target, "sending vote");
        let envelope = RaftRpc::Vote(rpc);
        match self.send_rpc(&envelope).await? {
            RaftRpcResponse::Vote(r) => r.map_err(|e| {
                RPCError::RemoteError(openraft::error::RemoteError::new(self.target, e))
            }),
            other => Err(RPCError::Network(NetworkError::new(&TransportError(format!(
                "unexpected response variant: expected Vote, got {other:?}"
            ))))),
        }
    }
}
