"""
Test 13: 307 redirect for follower write requests.

Verifies:
  1. Followers return 307 Temporary Redirect for write requests
     instead of proxying, with correct Location and custom headers.
  2. The redirect + re-send path actually indexes documents end-to-end.
  3. Leaders accept writes directly without redirecting.
"""

import time

import requests


def _find_leader_and_follower(nodes):
    """Return (leader_node, follower_node) from a list of nodes."""
    time.sleep(3)  # cluster stabilization
    for node in nodes:
        status = node.get_status()
        if status.get("lifecycle") == "leader":
            leader = node
            break
    else:
        raise RuntimeError("No leader found")
    follower = next(n for n in nodes if n.node_id != leader.node_id)
    return leader, follower


def test_follower_returns_307_redirect(node_factory):
    """A write to a follower should return 307 with Location pointing to leader."""
    n1 = node_factory(node_id=1)
    cluster_key = n1.start_create()
    n1.wait_healthy()

    n2 = node_factory(node_id=2)
    n2.start_join(n1.quic_addr, cluster_key)
    n2.wait_healthy()

    leader, follower = _find_leader_and_follower([n1, n2])

    # Send a write to the follower with allow_redirects=False
    resp = requests.post(
        f"{follower.url}/indexes/redirect-test/documents",
        headers={**follower.headers, "Content-Type": "application/json"},
        json=[{"id": 1, "title": "Test"}],
        allow_redirects=False,
        timeout=10,
    )

    assert resp.status_code == 307, (
        f"Expected 307 from follower, got {resp.status_code}: {resp.text}"
    )

    # Verify Location header points to leader
    location = resp.headers.get("Location")
    assert location is not None, "307 response missing Location header"
    assert location.startswith(leader.url), (
        f"Location should point to leader {leader.url}, got: {location}"
    )
    assert "/indexes/redirect-test/documents" in location, (
        f"Location should preserve path, got: {location}"
    )

    # Verify custom header for client caching
    leader_header = resp.headers.get("X-Meili-Cluster-Leader")
    assert leader_header is not None, "307 response missing X-Meili-Cluster-Leader header"
    assert leader.url in leader_header, (
        f"X-Meili-Cluster-Leader should contain leader URL, got: {leader_header}"
    )

    # Verify JSON body has leader info for non-redirect-following clients
    body = resp.json()
    assert "leaderUrl" in body, f"307 body should contain leaderUrl: {body}"
    assert "location" in body, f"307 body should contain location: {body}"


def test_redirect_write_indexes_end_to_end(node_factory):
    """Writing via a follower (with redirect handling) should index and replicate."""
    n1 = node_factory(node_id=1)
    cluster_key = n1.start_create()
    n1.wait_healthy()

    n2 = node_factory(node_id=2)
    n2.start_join(n1.quic_addr, cluster_key)
    n2.wait_healthy()

    leader, follower = _find_leader_and_follower([n1, n2])

    # Use conftest's _request helper (follows 307 with auth) via add_documents
    docs = [
        {"id": 1, "title": "Redirect Test Doc", "author": "Test Author"},
        {"id": 2, "title": "Another Doc", "author": "Another Author"},
    ]
    task_uid = follower.add_documents("redir-e2e", docs)
    result = follower.wait_task(task_uid, timeout=30)
    assert result["status"] == "succeeded", f"Task failed: {result}"

    time.sleep(3)  # replication

    # Both nodes should have the documents
    for node in [leader, follower]:
        results = node.search("redir-e2e", "redirect")
        assert results["estimatedTotalHits"] >= 1, (
            f"Node {node.node_id}: expected hit for 'redirect', got {results}"
        )


def test_leader_does_not_redirect(node_factory):
    """A write to the leader should NOT return 307."""
    n1 = node_factory(node_id=1)
    cluster_key = n1.start_create()
    n1.wait_healthy()

    n2 = node_factory(node_id=2)
    n2.start_join(n1.quic_addr, cluster_key)
    n2.wait_healthy()

    leader, _ = _find_leader_and_follower([n1, n2])

    resp = requests.post(
        f"{leader.url}/indexes/no-redir/documents",
        headers={**leader.headers, "Content-Type": "application/json"},
        json=[{"id": 1, "title": "Direct"}],
        allow_redirects=False,
        timeout=10,
    )

    # Leader should accept the write directly (202 Accepted)
    assert resp.status_code != 307, (
        f"Leader should not redirect writes, got 307 to {resp.headers.get('Location')}"
    )
    assert resp.status_code in (200, 202), (
        f"Leader should accept writes, got {resp.status_code}: {resp.text}"
    )
