"""
Test 08: Concurrent writes from multiple clients.

Scenario:
  1. Create a 3-node cluster
  2. Use threading to send 5 concurrent document additions to different indexes
  3. Some to leader, some to followers (testing write forwarding under concurrency)
  4. Wait for all tasks, verify all data searchable on all nodes
"""

import threading
import time


def test_concurrent_writes_to_different_indexes(node_factory):
    """Concurrent writes to different indexes should all succeed via Raft serialization."""
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

    time.sleep(3)  # cluster stabilization

    # Define 5 concurrent write tasks targeting different indexes and nodes
    writes = [
        (n1, "index-a", [{"id": i, "text": f"alpha-{i}"} for i in range(10)]),
        (n2, "index-b", [{"id": i, "text": f"beta-{i}"} for i in range(10)]),   # via follower
        (n3, "index-c", [{"id": i, "text": f"gamma-{i}"} for i in range(10)]),  # via follower
        (n1, "index-d", [{"id": i, "text": f"delta-{i}"} for i in range(10)]),
        (n2, "index-e", [{"id": i, "text": f"epsilon-{i}"} for i in range(10)]),  # via follower
    ]

    results = {}
    errors = []

    def do_write(node, index, docs, slot):
        try:
            task_uid = node.add_documents(index, docs)
            results[slot] = (node, task_uid)
        except Exception as e:
            errors.append((slot, str(e)))

    threads = []
    for i, (node, index, docs) in enumerate(writes):
        t = threading.Thread(target=do_write, args=(node, index, docs, i))
        threads.append(t)

    # Start all threads simultaneously
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=60)

    assert not errors, f"Write errors: {errors}"
    assert len(results) == 5, f"Expected 5 results, got {len(results)}"

    # Wait for all tasks to succeed (check on leader)
    for slot, (node, task_uid) in results.items():
        result = n1.wait_task(task_uid, timeout=60)
        assert result["status"] == "succeeded", (
            f"Slot {slot}: task {task_uid} failed: {result}"
        )

    # Wait for replication
    time.sleep(5)

    # Verify all data searchable on all nodes
    indexes = ["index-a", "index-b", "index-c", "index-d", "index-e"]
    for node in [n1, n2, n3]:
        for index in indexes:
            results = node.search(index, "")
            assert results["estimatedTotalHits"] == 10, (
                f"Node {node.node_id}, {index}: expected 10 hits, "
                f"got {results['estimatedTotalHits']}"
            )
