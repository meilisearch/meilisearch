"""
Test 11: Barrier header (read-after-write consistency).

Scenario:
  1. Create a 3-node cluster
  2. Add documents to leader, capture returned taskUid
  3. Immediately query a follower with X-Meili-Barrier: <taskUid>
  4. Verify the follower returns results (barrier waited for replication)
"""

import time

import requests


def test_barrier_read_after_write(node_factory):
    """X-Meili-Barrier header should make follower wait for the specified task before responding."""
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

    # Add initial documents and wait
    docs = [{"id": i, "title": f"Baseline doc {i}"} for i in range(5)]
    task_uid = n1.add_documents("barrier-test", docs)
    result = n1.wait_task(task_uid)
    assert result["status"] == "succeeded"
    time.sleep(3)

    # Now add NEW documents and immediately query follower with barrier
    new_docs = [{"id": 100 + i, "title": f"Barrier doc {i}"} for i in range(5)]
    task_uid = n1.add_documents("barrier-test", new_docs)

    # Wait for the task to succeed on leader first (the barrier on follower
    # needs the task to be processed, not just enqueued)
    result = n1.wait_task(task_uid, timeout=30)
    assert result["status"] == "succeeded"

    # Immediately query follower WITH the barrier header.
    # Barrier format is "indexName=taskUid", e.g., "barrier-test=5"
    barrier_value = f"barrier-test={task_uid}"
    resp = requests.post(
        f"{n2.url}/indexes/barrier-test/search",
        headers={
            **n2.headers,
            "Content-Type": "application/json",
            "X-Meili-Barrier": barrier_value,
        },
        json={"q": "Barrier"},
        timeout=30,  # generous timeout for barrier wait
    )
    resp.raise_for_status()
    results = resp.json()

    # The barrier should have ensured the follower sees the new documents
    assert results["estimatedTotalHits"] >= 1, (
        f"Barrier search: expected hits for 'Barrier' docs, got {results['estimatedTotalHits']}"
    )

    # Also verify total count includes all documents
    resp = requests.post(
        f"{n2.url}/indexes/barrier-test/search",
        headers={
            **n2.headers,
            "Content-Type": "application/json",
            "X-Meili-Barrier": barrier_value,
        },
        json={"q": ""},
        timeout=30,
    )
    resp.raise_for_status()
    results = resp.json()
    assert results["estimatedTotalHits"] == 10, (
        f"Expected 10 total docs after barrier, got {results['estimatedTotalHits']}"
    )
