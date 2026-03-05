"""
Test 10: Settings and API key replication.

Scenario:
  1. Create 3-node cluster, add documents
  2. Update index settings (filterable attributes) via leader
  3. Verify settings propagated to followers
  4. Create an API key via leader, verify it works on a follower
  5. Delete the key, verify it's rejected on all nodes
"""

import time

import requests


def test_settings_replication(node_factory):
    """Index settings updated on leader should propagate to all followers."""
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

    # Add documents with a filterable field
    docs = [
        {"id": 1, "title": "Python Guide", "category": "programming", "year": 2024},
        {"id": 2, "title": "Rust Guide", "category": "programming", "year": 2025},
        {"id": 3, "title": "Cooking 101", "category": "cooking", "year": 2023},
    ]
    task_uid = n1.add_documents("guides", docs)
    result = n1.wait_task(task_uid)
    assert result["status"] == "succeeded"

    # Update settings: add filterable attributes
    resp = requests.patch(
        f"{n1.url}/indexes/guides/settings",
        headers={**n1.headers, "Content-Type": "application/json"},
        json={"filterableAttributes": ["category", "year"]},
        timeout=10,
    )
    resp.raise_for_status()
    settings_task_uid = resp.json()["taskUid"]
    result = n1.wait_task(settings_task_uid, timeout=60)
    assert result["status"] == "succeeded", f"Settings task failed: {result}"

    # Wait for replication
    time.sleep(5)

    # Verify settings on all nodes
    for node in [n1, n2, n3]:
        resp = requests.get(
            f"{node.url}/indexes/guides/settings/filterable-attributes",
            headers=node.headers,
            timeout=5,
        )
        resp.raise_for_status()
        filterable = resp.json()
        assert set(filterable) == {"category", "year"}, (
            f"Node {node.node_id}: filterable attributes mismatch: {filterable}"
        )

    # Verify filtering works on a follower
    results = n2.search("guides", "", filter="category = programming")
    assert results["estimatedTotalHits"] == 2, (
        f"Filter search on follower: expected 2 hits, got {results['estimatedTotalHits']}"
    )


def test_api_key_replication(node_factory):
    """API keys created on leader should work on followers; deleted keys should be rejected."""
    # Create 3-node cluster
    n1 = node_factory(node_id=1)
    cluster_key = n1.start_create()
    n1.wait_healthy()

    n2 = node_factory(node_id=2)
    n2.start_join(n1.quic_addr, cluster_key)
    n2.wait_healthy()

    time.sleep(3)

    # Add a document so we have something to search
    task_uid = n1.add_documents("keytest", [{"id": 1, "title": "Test"}])
    result = n1.wait_task(task_uid)
    assert result["status"] == "succeeded"

    time.sleep(3)

    # Create an API key on leader
    resp = requests.post(
        f"{n1.url}/keys",
        headers={**n1.headers, "Content-Type": "application/json"},
        json={
            "description": "Test search key",
            "actions": ["search"],
            "indexes": ["keytest"],
            "expiresAt": None,
        },
        timeout=10,
    )
    resp.raise_for_status()
    key_data = resp.json()
    api_key = key_data["key"]
    key_uid = key_data["uid"]

    # Wait for key replication
    time.sleep(5)

    # Verify the key works on the follower
    resp = requests.post(
        f"{n2.url}/indexes/keytest/search",
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        json={"q": "Test"},
        timeout=10,
    )
    assert resp.status_code == 200, (
        f"API key should work on follower, got {resp.status_code}: {resp.text}"
    )

    # Delete the key on leader
    resp = requests.delete(
        f"{n1.url}/keys/{key_uid}",
        headers=n1.headers,
        timeout=10,
    )
    assert resp.status_code == 204, f"Key delete failed: {resp.status_code}"

    # Wait for deletion to replicate
    time.sleep(5)

    # Verify the key is rejected on all nodes
    for node in [n1, n2]:
        resp = requests.post(
            f"{node.url}/indexes/keytest/search",
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            json={"q": "Test"},
            timeout=10,
        )
        assert resp.status_code in (401, 403), (
            f"Node {node.node_id}: deleted key should be rejected, got {resp.status_code}"
        )
