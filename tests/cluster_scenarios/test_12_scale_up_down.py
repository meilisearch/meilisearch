"""
Test 12: Scale up and scale down — grow and shrink a cluster dynamically.

Scenario:
  1. Start a single-node cluster (node 1) and feed documents
  2. Add node 2, wait for it to join, verify both nodes work
  3. Add node 3, wait for it to join, verify all three nodes work
  4. Remove node 3 (graceful leave), verify 2-node cluster works
  5. Remove node 2 (graceful leave), verify single-node cluster works

A single-node cluster can always serve reads and writes.
A two-node cluster can serve reads and writes while both nodes are up
(but would lose quorum if one disconnects, since 2/2 is needed).
"""

import time


def wait_for_doc_count(nodes, index, expected, timeout=30, label=""):
    """Wait until all nodes report the expected document count."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        all_ok = True
        for node in nodes:
            try:
                stats = node.get_index_stats(index)
                if stats["numberOfDocuments"] != expected:
                    all_ok = False
                    break
            except Exception:
                all_ok = False
                break
        if all_ok:
            return
        time.sleep(2)
    # Final assertion with details
    for node in nodes:
        try:
            stats = node.get_index_stats(index)
        except Exception as e:
            raise AssertionError(
                f"{label}: Node {node.node_id} stats failed: {e}"
            )
        assert stats["numberOfDocuments"] == expected, (
            f"{label}: Node {node.node_id}: expected {expected} docs, "
            f"got {stats['numberOfDocuments']}"
        )


def test_scale_up_then_down(node_factory):
    """Grow from 1 to 3 nodes while indexing, then shrink back to 1."""

    # --- Phase 1: single-node cluster, feed initial documents ---
    n1 = node_factory(node_id=1)
    cluster_key = n1.start_create()
    n1.wait_healthy()

    docs_batch1 = [{"id": i, "title": f"Phase 1 doc {i}"} for i in range(100)]
    task_uid = n1.add_documents("scale", docs_batch1)
    result = n1.wait_task(task_uid)
    assert result["status"] == "succeeded", f"Batch 1 failed: {result}"

    wait_for_doc_count([n1], "scale", 100, label="Phase 1")

    # --- Phase 2: add node 2, feed more documents ---
    n2 = node_factory(node_id=2)
    n2.start_join(n1.quic_addr, cluster_key)
    n2.wait_healthy()

    time.sleep(5)  # let join/promotion and initial state transfer settle

    status = n1.get_status()
    assert 2 in status.get("voters", []), (
        f"Node 2 not in voters after join: {status.get('voters')}"
    )

    docs_batch2 = [{"id": 100 + i, "title": f"Phase 2 doc {i}"} for i in range(100)]
    task_uid = n1.add_documents("scale", docs_batch2)
    result = n1.wait_task(task_uid)
    assert result["status"] == "succeeded", f"Batch 2 failed: {result}"

    # Wait for replication including catch-up of pre-join data
    wait_for_doc_count([n1, n2], "scale", 200, timeout=30, label="Phase 2")

    # --- Phase 3: add node 3, feed more documents ---
    n3 = node_factory(node_id=3)
    n3.start_join(n1.quic_addr, cluster_key)
    n3.wait_healthy()

    time.sleep(5)  # let join/promotion settle

    status = n1.get_status()
    assert 3 in status.get("voters", []), (
        f"Node 3 not in voters after join: {status.get('voters')}"
    )

    docs_batch3 = [{"id": 200 + i, "title": f"Phase 3 doc {i}"} for i in range(100)]
    task_uid = n1.add_documents("scale", docs_batch3)
    result = n1.wait_task(task_uid)
    assert result["status"] == "succeeded", f"Batch 3 failed: {result}"

    wait_for_doc_count([n1, n2, n3], "scale", 300, timeout=30, label="Phase 3")

    # Verify search works on all nodes
    for node in [n1, n2, n3]:
        results = node.search("scale", "Phase")
        assert results["estimatedTotalHits"] >= 1, (
            f"Node {node.node_id}: search returned no results"
        )

    # --- Phase 4: remove node 3 (graceful leave) ---
    resp = n3.leave()
    assert resp.status_code == 200, f"Node 3 leave failed: {resp.status_code} {resp.text}"

    time.sleep(5)

    status = n1.get_status()
    voters = status.get("voters", [])
    assert 3 not in voters, f"Node 3 still in voters after leave: {voters}"
    assert len(voters) == 2, f"Expected 2 voters, got {voters}"

    # Write should still work on 2-node cluster
    docs_batch4 = [{"id": 300 + i, "title": f"Phase 4 doc {i}"} for i in range(50)]
    task_uid = n1.add_documents("scale", docs_batch4)
    result = n1.wait_task(task_uid)
    assert result["status"] == "succeeded", f"Batch 4 (after node 3 leave) failed: {result}"

    wait_for_doc_count([n1, n2], "scale", 350, timeout=20, label="Phase 4")

    # --- Phase 5: remove node 2 (graceful leave) → back to single-node ---
    resp = n2.leave()
    assert resp.status_code == 200, f"Node 2 leave failed: {resp.status_code} {resp.text}"

    time.sleep(5)

    status = n1.get_status()
    voters = status.get("voters", [])
    assert 2 not in voters, f"Node 2 still in voters after leave: {voters}"
    assert len(voters) == 1, f"Expected 1 voter, got {voters}"

    # Single-node cluster should still serve reads and writes
    docs_batch5 = [{"id": 350 + i, "title": f"Phase 5 doc {i}"} for i in range(50)]
    task_uid = n1.add_documents("scale", docs_batch5)
    result = n1.wait_task(task_uid)
    assert result["status"] == "succeeded", f"Batch 5 (single node) failed: {result}"

    wait_for_doc_count([n1], "scale", 400, label="Phase 5")

    # Final search verification
    results = n1.search("scale", "Phase")
    assert results["estimatedTotalHits"] >= 1, "Final search returned no results"
