"""
Test 09: Write-forwarding a large document via a follower.

Scenario:
  1. Create a 3-node cluster
  2. Add a single document with a 5MB text field via a follower
  3. Verify the document arrives on all nodes
"""

import time


def test_large_document_via_follower(node_factory):
    """A large document (5MB) sent via a follower should arrive on all nodes."""
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

    # Create a ~5MB document
    large_text = "word " * (5 * 1024 * 1024 // 5)  # ~5MB of text
    doc = [{"id": 1, "title": "Large Document", "body": large_text}]

    # Send via follower (n2) to test write forwarding + DML streaming
    task_uid = n2.add_documents("large-docs", doc)
    result = n2.wait_task(task_uid, timeout=120)
    assert result["status"] == "succeeded", f"Task failed: {result}"

    # Wait for replication
    time.sleep(5)

    # Verify on all nodes
    for node in [n1, n2, n3]:
        results = node.search("large-docs", "Large Document")
        assert results["estimatedTotalHits"] >= 1, (
            f"Node {node.node_id}: expected 1 hit for large document"
        )
        # Verify the document is actually there and has content
        hit = results["hits"][0]
        assert hit["title"] == "Large Document", (
            f"Node {node.node_id}: title mismatch"
        )
