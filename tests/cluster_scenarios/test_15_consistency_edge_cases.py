"""
Test 15: Consistency edge cases — leader crash mid-write, concurrent writes
to the same index, and leader crash mid-batch.

These tests cover the documented gaps from documentation/cluster.md under
"Consistency Testing > What we don't test":

  1. Leader crash mid-write — verify committed writes survive leader failure
  2. Concurrent writes to the same index — verify all writes are applied
  3. Leader crash mid-batch — verify a new leader completes outstanding work
"""

import threading
import time


def _create_three_node_cluster(node_factory):
    """Helper: spin up a 3-node cluster and return (n1, n2, n3, cluster_key)."""
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
    return n1, n2, n3, cluster_key


def _find_leader_and_survivors(nodes_dict):
    """Identify the current leader and return (leader_id, leader_node, survivors_dict).

    nodes_dict: {node_id: MeilisearchNode, ...}
    """
    # Ask any node for cluster status
    for node in nodes_dict.values():
        try:
            status = node.get_status()
            leader_id = status.get("raftLeaderId")
            if leader_id is not None:
                leader_node = nodes_dict[leader_id]
                survivors = {nid: n for nid, n in nodes_dict.items() if nid != leader_id}
                return leader_id, leader_node, survivors
        except Exception:
            continue
    raise RuntimeError("Could not determine leader")


def _wait_for_new_leader(survivors, old_leader_id, timeout=30):
    """Wait for a new leader to be elected among the surviving nodes.

    Returns (new_leader_id, new_leader_node).
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        for nid, node in survivors.items():
            try:
                status = node.get_status()
                new_id = status.get("raftLeaderId")
                if new_id is not None and new_id != old_leader_id:
                    return new_id, survivors[new_id]
            except Exception:
                pass
        time.sleep(1)
    raise RuntimeError(f"No new leader elected within {timeout}s (old leader was {old_leader_id})")


# ---------------------------------------------------------------------------
# Test 1: Leader crash mid-write
# ---------------------------------------------------------------------------


def test_committed_write_survives_leader_crash(node_factory):
    """A write committed by Raft must survive leader failure.

    Steps:
      1. Start a 3-node cluster
      2. Add documents and wait for the task to be acknowledged (succeeded)
      3. Kill the leader immediately after the task succeeds
      4. Wait for a new leader to be elected
      5. Verify the documents are present on the surviving nodes
    """
    n1, n2, n3, _ = _create_three_node_cluster(node_factory)
    nodes = {1: n1, 2: n2, 3: n3}

    # Submit a document add and wait for Raft to commit it (task succeeded)
    docs = [
        {"id": 1, "title": "Committed Before Crash", "author": "Test Author"},
        {"id": 2, "title": "Also Committed", "author": "Another Author"},
        {"id": 3, "title": "Third Document", "author": "Third Author"},
    ]

    leader_id, leader_node, survivors = _find_leader_and_survivors(nodes)

    task_uid = leader_node.add_documents("crash-test", docs)
    result = leader_node.wait_task(task_uid, timeout=30)
    assert result["status"] == "succeeded", f"Task did not succeed before crash: {result}"

    # Kill the leader immediately after the write is confirmed
    leader_node.kill()

    # Wait for a new leader to emerge
    new_leader_id, new_leader = _wait_for_new_leader(survivors, leader_id)
    assert new_leader_id != leader_id, "New leader must be different from crashed leader"

    # Wait a bit for the new leader to settle
    time.sleep(3)

    # Verify the committed documents are present on both surviving nodes
    for nid, node in survivors.items():
        stats = node.get_index_stats("crash-test")
        assert stats["numberOfDocuments"] == 3, (
            f"Node {nid}: expected 3 docs after leader crash, "
            f"got {stats['numberOfDocuments']}"
        )

        # Verify searchability
        results = node.search("crash-test", "Committed")
        assert results["estimatedTotalHits"] >= 1, (
            f"Node {nid}: 'Committed Before Crash' not searchable after leader crash"
        )

    # Verify the new leader can still accept writes
    new_docs = [{"id": 4, "title": "Post-Crash Document", "author": "Resilient Writer"}]
    task_uid = new_leader.add_documents("crash-test", new_docs)
    result = new_leader.wait_task(task_uid, timeout=30)
    assert result["status"] == "succeeded", f"Post-crash write failed: {result}"

    time.sleep(2)

    # All surviving nodes should now have 4 documents
    for nid, node in survivors.items():
        stats = node.get_index_stats("crash-test")
        assert stats["numberOfDocuments"] == 4, (
            f"Node {nid}: expected 4 docs after post-crash write, "
            f"got {stats['numberOfDocuments']}"
        )


# ---------------------------------------------------------------------------
# Test 2: Concurrent writes to the same index
# ---------------------------------------------------------------------------


def test_concurrent_writes_to_same_index(node_factory):
    """Concurrent writes from multiple clients to the same index should all be applied.

    Raft serializes writes through the leader, so all N document additions
    should eventually appear. This test verifies correctness under contention.

    Steps:
      1. Start a 3-node cluster
      2. Submit 10 document additions concurrently (different document IDs)
         spread across all nodes (leader and followers)
      3. Wait for all tasks to complete
      4. Verify all 10 documents are present on all nodes
    """
    n1, n2, n3, _ = _create_three_node_cluster(node_factory)

    num_concurrent_writes = 10
    target_nodes = [n1, n2, n3]

    # Each concurrent write adds a single document with a unique ID
    results = {}
    errors = []

    def do_write(slot, node, doc):
        try:
            task_uid = node.add_documents("concurrent-idx", [doc])
            results[slot] = task_uid
        except Exception as e:
            errors.append((slot, str(e)))

    threads = []
    for i in range(num_concurrent_writes):
        doc = {"id": i, "title": f"Concurrent doc {i}", "value": i * 100}
        # Round-robin across nodes so some go to followers (triggering redirect)
        target = target_nodes[i % len(target_nodes)]
        t = threading.Thread(target=do_write, args=(i, target, doc))
        threads.append(t)

    # Start all threads at roughly the same time
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=60)

    assert not errors, f"Write errors: {errors}"
    assert len(results) == num_concurrent_writes, (
        f"Expected {num_concurrent_writes} results, got {len(results)}"
    )

    # Wait for all tasks to succeed (check on leader)
    for slot, task_uid in results.items():
        result = n1.wait_task(task_uid, timeout=60)
        assert result["status"] == "succeeded", (
            f"Slot {slot}: task {task_uid} failed: {result}"
        )

    # Wait for replication
    time.sleep(5)

    # Verify all documents are present on all nodes
    for node in [n1, n2, n3]:
        stats = node.get_index_stats("concurrent-idx")
        assert stats["numberOfDocuments"] == num_concurrent_writes, (
            f"Node {node.node_id}: expected {num_concurrent_writes} docs, "
            f"got {stats['numberOfDocuments']}"
        )

        # Verify we can find each document
        search_results = node.search("concurrent-idx", "", limit=num_concurrent_writes + 5)
        assert search_results["estimatedTotalHits"] == num_concurrent_writes, (
            f"Node {node.node_id}: search expected {num_concurrent_writes} hits, "
            f"got {search_results['estimatedTotalHits']}"
        )


# ---------------------------------------------------------------------------
# Test 3: Leader crash mid-batch
# ---------------------------------------------------------------------------


def test_leader_crash_mid_batch(node_factory):
    """If the leader dies during batch processing, the new leader must complete the work.

    Steps:
      1. Start a 3-node cluster
      2. Submit a large document set (many batches) to the leader
      3. Kill the leader while processing is still ongoing
      4. Wait for a new leader to be elected
      5. Verify the new leader eventually processes all committed tasks
    """
    n1, n2, n3, _ = _create_three_node_cluster(node_factory)
    nodes = {1: n1, 2: n2, 3: n3}

    leader_id, leader_node, survivors = _find_leader_and_survivors(nodes)

    # Submit many batches rapidly so there is work in flight when we kill
    total_docs = 5000
    batch_size = 500
    task_uids = []

    for batch_start in range(0, total_docs, batch_size):
        docs = [
            {
                "id": batch_start + i,
                "title": f"Batch doc {batch_start + i}",
                "body": f"Content for document number {batch_start + i} with filler text.",
            }
            for i in range(batch_size)
        ]
        task_uid = leader_node.add_documents("mid-batch", docs)
        task_uids.append(task_uid)

    # Give a moment for some tasks to start processing, then kill the leader.
    # We want to catch it mid-processing: some tasks committed but not all
    # completed yet.
    time.sleep(2)
    leader_node.kill()

    # Wait for new leader election
    new_leader_id, new_leader = _wait_for_new_leader(survivors, leader_id)

    # The new leader should eventually process all committed tasks.
    # Some tasks may have been committed before the crash; those must complete.
    # Tasks that were NOT committed (not yet replicated to quorum) may be lost,
    # which is acceptable per Raft semantics.
    #
    # We wait for all tasks that the new leader knows about to finish.
    # First, let's see which tasks the new leader has.
    time.sleep(5)  # let the new leader settle

    succeeded_count = 0
    for task_uid in task_uids:
        try:
            result = new_leader.wait_task(task_uid, timeout=60)
            if result["status"] == "succeeded":
                succeeded_count += 1
        except Exception:
            # Task may not exist if it wasn't committed before crash
            pass

    # At least some tasks must have been committed and processed
    assert succeeded_count >= 1, (
        f"No tasks succeeded after leader crash mid-batch. "
        f"Expected at least some committed tasks to survive."
    )

    # Wait for replication to finish
    time.sleep(5)

    # Verify both survivors agree on the document count
    survivor_counts = {}
    for nid, node in survivors.items():
        try:
            stats = node.get_index_stats("mid-batch")
            survivor_counts[nid] = stats["numberOfDocuments"]
        except Exception as e:
            survivor_counts[nid] = f"error: {e}"

    # Both survivors must have the same number of documents
    count_values = [v for v in survivor_counts.values() if isinstance(v, int)]
    assert len(count_values) == 2, (
        f"Could not get doc counts from both survivors: {survivor_counts}"
    )
    assert count_values[0] == count_values[1], (
        f"Survivors disagree on document count: {survivor_counts}"
    )
    assert count_values[0] > 0, (
        f"Expected some documents to survive, got {count_values[0]}"
    )

    # Verify search works on the new leader
    results = new_leader.search("mid-batch", "Batch doc")
    assert results["estimatedTotalHits"] >= 1, (
        "No searchable documents on new leader after mid-batch crash"
    )

    # The new leader should still be able to accept new writes
    new_docs = [{"id": 99999, "title": "Post mid-batch crash doc"}]
    task_uid = new_leader.add_documents("mid-batch", new_docs)
    result = new_leader.wait_task(task_uid, timeout=30)
    assert result["status"] == "succeeded", f"Post mid-batch crash write failed: {result}"
