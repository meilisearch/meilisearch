"""
Test 06: Simulate node failure, verify eventual eviction (if configured).

Scenario:
  1. Create a 3-node cluster with a short lag-eviction threshold
  2. Kill a follower (SIGKILL, no graceful leave)
  3. Wait for the eviction timeout
  4. Verify the dead node is removed from membership
  5. Verify writes still succeed on the 2-node cluster

Note: This test depends on the lag eviction feature being active.
The --cluster-max-replication-lag controls the log-entry threshold.
"""

import time


EVICTION_WAIT_SECS = 45  # Max time to wait for eviction


def test_dead_node_eventually_evicted(node_factory):
    """A node that dies without leaving should eventually be evicted."""
    # Use a low replication lag threshold for faster eviction
    extra = ["--cluster-max-replication-lag", "100"]

    n1 = node_factory(node_id=1, extra_args=extra)
    cluster_key = n1.start_create()
    n1.wait_healthy()

    n2 = node_factory(node_id=2, extra_args=extra)
    n2.start_join(n1.quic_addr, cluster_key)
    n2.wait_healthy()

    n3 = node_factory(node_id=3, extra_args=extra)
    n3.start_join(n1.quic_addr, cluster_key)
    n3.wait_healthy()

    time.sleep(3)

    # Verify 3 voters
    status = n1.get_status()
    assert len(status.get("voters", [])) == 3

    # Kill node 3 abruptly (no graceful leave)
    n3.kill()

    # Wait for eviction — the leader should detect node 3 is lagging and evict it
    evicted = False
    deadline = time.time() + EVICTION_WAIT_SECS
    while time.time() < deadline:
        try:
            status = n1.get_status()
            voters = status.get("voters", [])
            if 3 not in voters:
                evicted = True
                break
        except Exception:
            pass
        time.sleep(2)

    assert evicted, (
        f"Node 3 was not evicted within {EVICTION_WAIT_SECS}s. "
        f"Current voters: {status.get('voters', [])}"
    )

    # Verify remaining cluster works
    assert len(status.get("voters", [])) == 2

    # Writes should succeed on 2-node cluster
    docs = [{"id": 1, "title": "Post Eviction Write"}]
    task_uid = n1.add_documents("test", docs)
    result = n1.wait_task(task_uid, timeout=30)
    assert result["status"] == "succeeded", f"Write failed after eviction: {result}"
