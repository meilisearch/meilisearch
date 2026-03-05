"""
Test 02: Add documents via leader, verify searchable on all nodes.

Scenario:
  1. Create a 3-node cluster
  2. Add documents to an index via the leader
  3. Wait for task to succeed
  4. Search on all 3 nodes and verify results match
"""

import time


def test_add_documents_and_search_all_nodes(node_factory):
    """Documents added via leader should be searchable on all nodes."""
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

    # Add documents via node 1 (leader)
    docs = [
        {"id": 1, "title": "The Great Gatsby", "author": "F. Scott Fitzgerald"},
        {"id": 2, "title": "To Kill a Mockingbird", "author": "Harper Lee"},
        {"id": 3, "title": "1984", "author": "George Orwell"},
        {"id": 4, "title": "Pride and Prejudice", "author": "Jane Austen"},
    ]
    task_uid = n1.add_documents("books", docs)
    result = n1.wait_task(task_uid)
    assert result["status"] == "succeeded", f"Task failed: {result}"

    # Wait for replication to propagate
    time.sleep(3)

    # Search on all nodes
    for node in [n1, n2, n3]:
        results = node.search("books", "gatsby")
        assert results["estimatedTotalHits"] >= 1, (
            f"Node {node.node_id}: expected at least 1 hit for 'gatsby', got {results}"
        )
        assert any("Gatsby" in hit["title"] for hit in results["hits"]), (
            f"Node {node.node_id}: 'The Great Gatsby' not in results"
        )

    # Search for another term on all nodes
    for node in [n1, n2, n3]:
        results = node.search("books", "orwell")
        assert results["estimatedTotalHits"] >= 1, (
            f"Node {node.node_id}: expected at least 1 hit for 'orwell'"
        )


def test_add_documents_via_follower(node_factory):
    """Documents added via a follower should be forwarded to leader and replicated."""
    n1 = node_factory(node_id=1)
    cluster_key = n1.start_create()
    n1.wait_healthy()

    n2 = node_factory(node_id=2)
    n2.start_join(n1.quic_addr, cluster_key)
    n2.wait_healthy()

    time.sleep(3)

    # Add documents via node 2 (follower)
    docs = [
        {"id": 1, "title": "Dune", "author": "Frank Herbert"},
        {"id": 2, "title": "Neuromancer", "author": "William Gibson"},
    ]
    task_uid = n2.add_documents("scifi", docs)
    result = n2.wait_task(task_uid, timeout=30)
    assert result["status"] == "succeeded", f"Task failed: {result}"

    time.sleep(3)

    # Search on both nodes
    for node in [n1, n2]:
        results = node.search("scifi", "dune")
        assert results["estimatedTotalHits"] >= 1, (
            f"Node {node.node_id}: expected at least 1 hit for 'dune'"
        )
