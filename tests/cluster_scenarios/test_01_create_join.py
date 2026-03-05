"""
Test 01: Create a 3-node cluster and verify all nodes are healthy.

Scenario:
  1. Node 1 creates a new cluster
  2. Node 2 joins the cluster
  3. Node 3 joins the cluster
  4. All 3 nodes report healthy via /health
  5. /cluster/status shows 3 voters with a leader elected
"""

import time

import requests


def test_create_three_node_cluster(node_factory):
    """Create a 3-node cluster, verify all healthy and cluster status correct."""
    # Node 1: create cluster
    n1 = node_factory(node_id=1)
    cluster_key = n1.start_create()
    n1.wait_healthy()

    # Node 2: join
    n2 = node_factory(node_id=2)
    n2.start_join(n1.quic_addr, cluster_key)
    n2.wait_healthy()

    # Node 3: join
    n3 = node_factory(node_id=3)
    n3.start_join(n1.quic_addr, cluster_key)
    n3.wait_healthy()

    # Wait for cluster to stabilize (membership propagation)
    time.sleep(3)

    # Check cluster status on each node
    for node in [n1, n2, n3]:
        status = node.get_status()
        assert "raftLeaderId" in status, f"Node {node.node_id}: no raftLeaderId in status"
        assert status["raftLeaderId"] is not None, f"Node {node.node_id}: raftLeaderId is None"

        voters = status.get("voters", [])
        assert len(voters) == 3, (
            f"Node {node.node_id}: expected 3 voters, got {len(voters)}: {voters}"
        )
        assert set(voters) == {1, 2, 3}, (
            f"Node {node.node_id}: unexpected voter set: {voters}"
        )

        # Verify lifecycle field is present and valid
        lifecycle = status.get("lifecycle")
        assert lifecycle in ("leader", "follower"), (
            f"Node {node.node_id}: unexpected lifecycle: {lifecycle}"
        )


def test_create_single_node_cluster(node_factory):
    """A single-node cluster should be healthy and elect itself leader."""
    n1 = node_factory(node_id=1)
    cluster_key = n1.start_create()
    n1.wait_healthy()

    time.sleep(2)

    status = n1.get_status()
    assert status["raftLeaderId"] == 1, f"Single node should be leader, got: {status}"
    voters = status.get("voters", [])
    assert 1 in voters, f"Node 1 should be a voter: {status}"
    assert status.get("lifecycle") == "leader", (
        f"Single-node leader should have lifecycle 'leader', got: {status.get('lifecycle')}"
    )


def test_join_requires_correct_secret(node_factory):
    """Joining with wrong secret should fail (node doesn't become healthy)."""
    n1 = node_factory(node_id=1)
    n1.start_create()
    n1.wait_healthy()

    n2 = node_factory(node_id=2)
    # Use wrong secret
    n2.start_join(n1.quic_addr, "wrong-secret-value")

    # Node 2 should fail to join — process should exit or never become healthy
    time.sleep(5)
    poll = n2.process.poll()
    if poll is not None:
        # Process exited — expected behavior
        assert poll != 0, "Process should exit with non-zero on bad secret"
    else:
        # Process still running but should not be healthy with cluster status
        try:
            resp = requests.get(f"{n2.url}/health", timeout=2)
            # Even if HTTP is up, cluster status should not show 2 voters
            status = n1.get_status()
            voters = status.get("voters", [])
            assert 2 not in voters, "Node 2 should not be a voter with wrong secret"
        except requests.ConnectionError:
            pass  # Expected — node may not be serving HTTP
