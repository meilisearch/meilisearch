"""
Test 04: Kill and restart a follower, verify it catches up.

Scenario:
  1. Create a 3-node cluster
  2. Add documents, verify replicated
  3. Kill a follower (SIGKILL)
  4. Add more documents while follower is down
  5. Restart the follower (same data dir)
  6. Verify it catches up and has all documents
"""

import time


def test_follower_restart_catches_up(node_factory):
    """A restarted follower should catch up via Raft log replay."""
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
    docs1 = [
        {"id": 1, "title": "Neuromancer", "author": "William Gibson"},
        {"id": 2, "title": "Snow Crash", "author": "Neal Stephenson"},
    ]
    task_uid = n1.add_documents("scifi", docs1)
    result = n1.wait_task(task_uid)
    assert result["status"] == "succeeded"

    time.sleep(2)

    # Verify on n3 before killing it
    results = n3.search("scifi", "neuromancer")
    assert results["estimatedTotalHits"] >= 1

    # Kill n3
    n3.kill()

    # Add more documents while n3 is down
    docs2 = [
        {"id": 3, "title": "Hyperion", "author": "Dan Simmons"},
        {"id": 4, "title": "The Left Hand of Darkness", "author": "Ursula K. Le Guin"},
    ]
    task_uid = n1.add_documents("scifi", docs2)
    result = n1.wait_task(task_uid, timeout=30)
    assert result["status"] == "succeeded"

    time.sleep(2)

    # Restart n3 (same data dir, auto-restart mode)
    n3.start_restart()
    n3.wait_healthy(timeout=30)

    # Wait for catch-up
    time.sleep(5)

    # Verify n3 has all documents (both batches)
    results = n3.search("scifi", "neuromancer")
    assert results["estimatedTotalHits"] >= 1, "Missing pre-restart document"

    results = n3.search("scifi", "hyperion")
    assert results["estimatedTotalHits"] >= 1, "Missing post-restart document (not caught up)"

    results = n3.search("scifi", "darkness")
    assert results["estimatedTotalHits"] >= 1, "Missing post-restart document (Le Guin)"
