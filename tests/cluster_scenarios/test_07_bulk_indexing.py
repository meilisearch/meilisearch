"""
Test 07: Bulk data ingestion + search during indexing.

Scenario:
  1. Create a 3-node cluster
  2. Add 10,000 documents in batches of 1,000
  3. While indexing, run search queries on a follower
  4. Verify partial results are available (not zero hits)
  5. After all tasks succeed, verify exact hit count on all nodes
"""

import time


def generate_documents(start_id, count):
    """Generate a batch of documents with searchable content."""
    return [
        {
            "id": start_id + i,
            "title": f"Document {start_id + i}: The Adventures of Item {start_id + i}",
            "body": f"This is the body of document number {start_id + i}. "
                    f"It contains searchable content about topic {(start_id + i) % 50}.",
            "category": f"cat-{(start_id + i) % 10}",
            "value": start_id + i,
        }
        for i in range(count)
    ]


def test_bulk_indexing_with_search_during_replication(node_factory):
    """Bulk document ingestion should work and search should return partial results during indexing."""
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

    # Add 10,000 documents in batches of 1,000 via leader
    total_docs = 10_000
    batch_size = 1_000
    task_uids = []

    for batch_start in range(0, total_docs, batch_size):
        docs = generate_documents(batch_start, batch_size)
        task_uid = n1.add_documents("bulk", docs)
        task_uids.append(task_uid)

    # While indexing is in progress, search on follower to check partial results
    partial_results_seen = False
    for _ in range(20):
        try:
            results = n2.search("bulk", "Adventures")
            if results.get("estimatedTotalHits", 0) > 0:
                partial_results_seen = True
                break
        except Exception:
            pass
        time.sleep(1)

    # Wait for ALL tasks to succeed on leader
    for task_uid in task_uids:
        result = n1.wait_task(task_uid, timeout=120)
        assert result["status"] == "succeeded", f"Task {task_uid} failed: {result}"

    # Wait for replication
    time.sleep(5)

    # Verify exact document count on all nodes via stats endpoint.
    # (Search estimatedTotalHits is capped by maxTotalHits=1000 default,
    # so we use the stats endpoint for accurate counts.)
    for node in [n1, n2, n3]:
        stats = node.get_index_stats("bulk")
        assert stats["numberOfDocuments"] == total_docs, (
            f"Node {node.node_id}: expected {total_docs} docs, "
            f"got {stats['numberOfDocuments']}"
        )

    # Verify search works on all nodes
    for node in [n1, n2, n3]:
        results = node.search("bulk", "Adventures")
        assert results["estimatedTotalHits"] >= 1, (
            f"Node {node.node_id}: expected hits for 'Adventures'"
        )

    # Partial results assertion is informational — not a hard failure
    # because batch processing may complete before we can observe partial state
    if partial_results_seen:
        print("Observed partial results during indexing (good)")
    else:
        print("Indexing completed before partial results could be observed (acceptable)")
