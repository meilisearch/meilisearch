"""
Test 05: A node gracefully leaves, verify cluster shrinks.

Scenario:
  1. Create a 3-node cluster
  2. Node 3 calls POST /cluster/status/leave
  3. Verify cluster membership shrinks to 2 voters
  4. Writes still succeed on the 2-node cluster
"""

import time

import requests


def test_graceful_leave(node_factory):
    """A node that gracefully leaves should be removed from membership."""
    n1 = node_factory(node_id=1)
    cluster_key = n1.start_create()
    n1.wait_healthy()

    n2 = node_factory(node_id=2)
    n2.start_join(n1.quic_addr, cluster_key)
    n2.wait_healthy()

    n3 = node_factory(node_id=3)
    n3.start_join(n1.quic_addr, cluster_key)
    n3.wait_healthy()

    time.sleep(3)

    # Verify 3 voters
    status = n1.get_status()
    assert len(status.get("voters", [])) == 3

    # Node 3 leaves gracefully
    resp = n3.leave()
    assert resp.status_code in (200, 202), f"Leave returned {resp.status_code}: {resp.text}"

    # Wait for membership change to propagate
    time.sleep(5)

    # Check that node 3's process has exited
    deadline = time.time() + 15
    while time.time() < deadline:
        if n3.process.poll() is not None:
            break
        time.sleep(1)

    # Verify cluster status on remaining nodes: 2 voters
    for node in [n1, n2]:
        status = node.get_status()
        voters = status.get("voters", [])
        assert 3 not in voters, (
            f"Node {node.node_id}: node 3 still in voters after leave: {voters}"
        )
        assert len(voters) == 2, (
            f"Node {node.node_id}: expected 2 voters, got {len(voters)}: {voters}"
        )

    # Writes still work on 2-node cluster
    docs = [{"id": 1, "title": "Test After Leave"}]
    task_uid = n1.add_documents("test", docs)
    result = n1.wait_task(task_uid, timeout=30)
    assert result["status"] == "succeeded", f"Write failed after leave: {result}"

    # Searchable on both remaining nodes
    time.sleep(2)
    for node in [n1, n2]:
        results = node.search("test", "leave")
        assert results["estimatedTotalHits"] >= 1, (
            f"Node {node.node_id}: document not searchable after leave"
        )


def test_leave_last_node_stays(node_factory):
    """The last node in a cluster should not be able to leave (would lose data)."""
    n1 = node_factory(node_id=1)
    n1.start_create()
    n1.wait_healthy()

    time.sleep(2)

    # Try to leave as the only node — the leave endpoint signals shutdown,
    # but leave() on the Raft node returns an error for a single-node cluster.
    # The node should still be running and healthy after the failed leave attempt.
    resp = n1.leave()
    # The endpoint returns 200 (signals shutdown) but the actual leave will fail
    # and the node continues running.
    time.sleep(3)

    # Node should still be reachable and healthy
    health = requests.get(f"{n1.url}/health", timeout=5)
    assert health.status_code == 200, (
        f"Node should still be healthy after failed last-node leave, got {health.status_code}"
    )
