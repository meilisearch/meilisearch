//! Integration tests for end-to-end cluster operations.
//!
//! Each test uses unique ports and temp directories to avoid conflicts.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use meilisearch_cluster::types::{RaftRequest, RaftResponse};
use meilisearch_cluster::{ClusterConfig, ClusterNode};

/// Pick a random available port by binding to :0 then releasing.
fn random_addr() -> SocketAddr {
    let socket = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    socket.local_addr().unwrap()
}

/// Fast test config with shorter timeouts for quicker elections.
fn test_config() -> ClusterConfig {
    ClusterConfig {
        heartbeat_ms: 200,
        election_timeout_min_ms: 500,
        election_timeout_max_ms: 1000,
        ..ClusterConfig::default()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_single_node_cluster() {
    let dir = tempfile::tempdir().unwrap();
    let addr = random_addr();

    let (node, _secret) = ClusterNode::create(
        0,
        addr,
        addr.to_string(),
        "http://127.0.0.1:7700".to_string(),
        dir.path(),
        &test_config(),
        None,
    )
    .await
    .expect("create cluster");

    let node = Arc::new(node);
    node.spawn_accept_loop();

    // Wait for leader election (single-node is instant)
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(node.is_leader(), "single node should be leader");
    assert_eq!(node.leader_id(), Some(0));

    // Write a Noop through Raft and verify it commits
    let resp = node.client_write(RaftRequest::Noop).await.expect("client_write");
    assert!(matches!(resp.data, RaftResponse::Ok));

    // Shut down cleanly
    node.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_two_node_cluster() {
    let dir1 = tempfile::tempdir().unwrap();
    let dir2 = tempfile::tempdir().unwrap();
    let addr1 = random_addr();
    let addr2 = random_addr();
    let cfg = test_config();

    // Create leader
    let (node1, secret) = ClusterNode::create(
        0,
        addr1,
        addr1.to_string(),
        "http://127.0.0.1:7700".to_string(),
        dir1.path(),
        &cfg,
        None,
    )
    .await
    .expect("create cluster");

    let node1 = Arc::new(node1);
    node1.spawn_accept_loop();

    // Wait for leader election
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(node1.is_leader());

    // Join a second node
    let node2 = ClusterNode::join(
        1,
        addr2,
        addr2.to_string(),
        "http://127.0.0.1:7701".to_string(),
        addr1,
        secret,
        dir2.path(),
        &cfg,
        env!("CARGO_PKG_VERSION"),
        vec![],
    )
    .await
    .expect("join cluster");

    let node2 = Arc::new(node2);
    node2.spawn_accept_loop();

    // Wait for membership propagation + election stabilization
    tokio::time::sleep(Duration::from_millis(1000)).await;

    // Node 0 should still be leader
    assert!(node1.is_leader(), "node1 should be leader");
    assert!(!node2.is_leader(), "node2 should be follower");
    assert_eq!(node2.leader_id(), Some(0));

    // Write through leader and verify commit
    let resp = node1.client_write(RaftRequest::Noop).await.expect("client_write");
    assert!(matches!(resp.data, RaftResponse::Ok));

    // Shut down both nodes
    node1.shutdown().await.expect("shutdown node1");
    node2.shutdown().await.expect("shutdown node2");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_task_replication_roundtrip() {
    let dir1 = tempfile::tempdir().unwrap();
    let dir2 = tempfile::tempdir().unwrap();
    let addr1 = random_addr();
    let addr2 = random_addr();
    let cfg = test_config();

    // Create leader
    let (node1, secret) = ClusterNode::create(
        0,
        addr1,
        addr1.to_string(),
        "http://127.0.0.1:7700".to_string(),
        dir1.path(),
        &cfg,
        None,
    )
    .await
    .expect("create cluster");

    let node1 = Arc::new(node1);
    node1.spawn_accept_loop();
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Join a second node
    let node2 = ClusterNode::join(
        1,
        addr2,
        addr2.to_string(),
        "http://127.0.0.1:7701".to_string(),
        addr1,
        secret,
        dir2.path(),
        &cfg,
        env!("CARGO_PKG_VERSION"),
        vec![],
    )
    .await
    .expect("join cluster");

    let node2 = Arc::new(node2);
    node2.spawn_accept_loop();
    tokio::time::sleep(Duration::from_millis(1000)).await;

    // Write a TaskEnqueued through Raft
    // (No TaskApplier set, so it gets buffered — we just verify the Raft log commits)
    let resp = node1
        .client_write(RaftRequest::TaskEnqueued { kind_bytes: vec![1, 2, 3, 4] })
        .await
        .expect("client_write TaskEnqueued");

    // Without a TaskApplier, the response is Ok (buffered), not TaskRegistered
    assert!(matches!(resp.data, RaftResponse::Ok));

    // Shut down both nodes
    node1.shutdown().await.expect("shutdown node1");
    node2.shutdown().await.expect("shutdown node2");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_three_node_leader_election() {
    let dirs: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
    let addrs: Vec<_> = (0..3).map(|_| random_addr()).collect();
    let cfg = test_config();

    // Create leader (node 0)
    let (node0, secret) = ClusterNode::create(
        0,
        addrs[0],
        addrs[0].to_string(),
        "http://127.0.0.1:7700".to_string(),
        dirs[0].path(),
        &cfg,
        None,
    )
    .await
    .expect("create cluster");

    let node0 = Arc::new(node0);
    node0.spawn_accept_loop();
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Join node 1
    let node1 = ClusterNode::join(
        1,
        addrs[1],
        addrs[1].to_string(),
        "http://127.0.0.1:7701".to_string(),
        addrs[0],
        secret.clone(),
        dirs[1].path(),
        &cfg,
        env!("CARGO_PKG_VERSION"),
        vec![],
    )
    .await
    .expect("join node 1");

    let node1 = Arc::new(node1);
    node1.spawn_accept_loop();
    tokio::time::sleep(Duration::from_millis(1000)).await;

    // Join node 2
    let node2 = ClusterNode::join(
        2,
        addrs[2],
        addrs[2].to_string(),
        "http://127.0.0.1:7702".to_string(),
        addrs[0],
        secret,
        dirs[2].path(),
        &cfg,
        env!("CARGO_PKG_VERSION"),
        vec![],
    )
    .await
    .expect("join node 2");

    let node2 = Arc::new(node2);
    node2.spawn_accept_loop();
    tokio::time::sleep(Duration::from_millis(1500)).await;

    // Verify initial state: node 0 is leader
    assert!(node0.is_leader());
    assert!(!node1.is_leader());
    assert!(!node2.is_leader());

    // All nodes agree on leader
    assert_eq!(node0.leader_id(), Some(0));
    assert_eq!(node1.leader_id(), Some(0));
    assert_eq!(node2.leader_id(), Some(0));

    // Shut down the leader
    node0.shutdown().await.expect("shutdown node0");

    // Wait for new leader election
    tokio::time::sleep(Duration::from_millis(3000)).await;

    // One of node1 or node2 should become leader
    let new_leader_id = node1.leader_id().or_else(|| node2.leader_id());
    assert!(new_leader_id.is_some(), "a new leader should have been elected after node0 shutdown");
    let new_leader_id = new_leader_id.unwrap();
    assert!(new_leader_id == 1 || new_leader_id == 2);

    // The new leader can accept writes
    let writing_node = if node1.is_leader() { &node1 } else { &node2 };
    let resp = writing_node.client_write(RaftRequest::Noop).await.expect("write to new leader");
    assert!(matches!(resp.data, RaftResponse::Ok));

    // Clean up
    node1.shutdown().await.expect("shutdown node1");
    node2.shutdown().await.expect("shutdown node2");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_single_node_restart_persistence() {
    let dir = tempfile::tempdir().unwrap();
    let secret;
    let cfg = test_config();

    // Phase 1: Create cluster, write entries, shut down
    {
        let addr = random_addr();
        let (node, s) = ClusterNode::create(
            0,
            addr,
            addr.to_string(),
            "http://127.0.0.1:7700".to_string(),
            dir.path(),
            &cfg,
            None,
        )
        .await
        .expect("create cluster");
        secret = s;

        let node = Arc::new(node);
        node.spawn_accept_loop();
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert!(node.is_leader());

        // Write a few entries through Raft
        for _ in 0..5 {
            let resp = node.client_write(RaftRequest::Noop).await.expect("client_write");
            assert!(matches!(resp.data, RaftResponse::Ok));
        }

        // Shut down — Raft state should be persisted in LMDB
        node.shutdown().await.expect("shutdown");
    }

    // Phase 2: Verify persisted config and restart from it
    {
        assert!(
            meilisearch_cluster::has_persisted_cluster(dir.path()),
            "should detect persisted cluster"
        );
        let config = meilisearch_cluster::load_node_config(dir.path())
            .expect("load config")
            .expect("config should exist");
        assert_eq!(config.node_id, 0);
        assert_eq!(config.secret, secret);

        let addr = random_addr(); // New port (old one may still be in TIME_WAIT)
        let node = ClusterNode::restart(config.node_id, addr, config.secret, dir.path(), &cfg)
            .await
            .expect("restart cluster");

        let node = Arc::new(node);
        node.spawn_accept_loop();
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Should become leader again (single-node cluster)
        assert!(node.is_leader(), "restarted node should be leader");

        // Should be able to write new entries (proving Raft resumed properly)
        let resp = node.client_write(RaftRequest::Noop).await.expect("write after restart");
        assert!(matches!(resp.data, RaftResponse::Ok));

        node.shutdown().await.expect("shutdown restarted node");
    }
}
