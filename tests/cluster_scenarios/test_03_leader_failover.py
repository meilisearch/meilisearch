"""
Test 03: Kill the leader, verify a new leader is elected and writes succeed.

Scenario:
  1. Create a 3-node cluster
  2. Add initial documents, verify on all nodes
  3. Kill the leader node (SIGKILL)
  4. Wait for a new leader election
  5. Add more documents via the new leader
  6. Verify new documents are searchable on surviving nodes
"""

import time


def test_leader_failover(node_factory):
    """Killing the leader should trigger election and allow continued writes."""
    # Create 3-node cluster
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

    # Add initial documents
    docs = [{"id": 1, "title": "Foundation", "author": "Isaac Asimov"}]
    task_uid = n1.add_documents("books", docs)
    result = n1.wait_task(task_uid)
    assert result["status"] == "succeeded", f"Initial task failed: {result}"

    time.sleep(2)

    # Identify the leader
    status = n1.get_status()
    leader_id = status["raftLeaderId"]
    assert leader_id is not None, "No leader elected"

    nodes = {1: n1, 2: n2, 3: n3}
    leader_node = nodes[leader_id]
    survivors = {nid: n for nid, n in nodes.items() if nid != leader_id}

    # Kill the leader
    leader_node.kill()

    # Wait for new leader election
    new_leader_id = None
    deadline = time.time() + 30
    while time.time() < deadline:
        for nid, node in survivors.items():
            try:
                status = node.get_status()
                if status.get("raftLeaderId") is not None and status["raftLeaderId"] != leader_id:
                    new_leader_id = status["raftLeaderId"]
                    break
            except Exception:
                pass
        if new_leader_id is not None:
            break
        time.sleep(1)

    assert new_leader_id is not None, "No new leader elected after killing old leader"
    assert new_leader_id in survivors, f"New leader {new_leader_id} is not a survivor"

    new_leader = survivors[new_leader_id]

    # Add more documents via new leader
    new_docs = [{"id": 2, "title": "Dune", "author": "Frank Herbert"}]
    task_uid = new_leader.add_documents("books", new_docs)
    result = new_leader.wait_task(task_uid, timeout=30)
    assert result["status"] == "succeeded", f"Post-failover task failed: {result}"

    time.sleep(2)

    # Verify on all surviving nodes
    for nid, node in survivors.items():
        results = node.search("books", "dune")
        assert results["estimatedTotalHits"] >= 1, (
            f"Node {nid}: 'Dune' not searchable after failover"
        )
        # Original data should still be there
        results = node.search("books", "foundation")
        assert results["estimatedTotalHits"] >= 1, (
            f"Node {nid}: 'Foundation' missing after failover"
        )
