"""
Test 14: Cluster health endpoints for load balancer routing.

Verifies:
  1. /cluster/health/writer returns 200 on leader, 503 on followers.
  2. /cluster/health/reader returns 200 on all healthy nodes.
  3. Health endpoints are unauthenticated (no Authorization header needed).
  4. Single-node cluster reports writer available.
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


def test_health_writer_leader_vs_follower(node_factory):
    """/cluster/health/writer: 200 on leader, 503 on follower."""
    n1 = node_factory(node_id=1)
    cluster_key = n1.start_create()
    n1.wait_healthy()

    n2 = node_factory(node_id=2)
    n2.start_join(n1.quic_addr, cluster_key)
    n2.wait_healthy()

    leader, follower = _find_leader_and_follower([n1, n2])

    # Leader should report writer capability available
    resp = requests.get(f"{leader.url}/cluster/health/writer", timeout=5)
    assert resp.status_code == 200, (
        f"Leader /cluster/health/writer should be 200, got {resp.status_code}: {resp.text}"
    )
    body = resp.json()
    assert body["status"] == "available"
    assert body["capability"] == "writer"
    assert body["role"] == "leader"

    # Follower should report writer capability unavailable
    resp = requests.get(f"{follower.url}/cluster/health/writer", timeout=5)
    assert resp.status_code == 503, (
        f"Follower /cluster/health/writer should be 503, got {resp.status_code}: {resp.text}"
    )
    body = resp.json()
    assert body["status"] == "unavailable"
    assert body["capability"] == "writer"
    assert body["role"] == "follower"


def test_health_reader_all_nodes(node_factory):
    """/cluster/health/reader: 200 on all healthy nodes (leader and follower)."""
    n1 = node_factory(node_id=1)
    cluster_key = n1.start_create()
    n1.wait_healthy()

    n2 = node_factory(node_id=2)
    n2.start_join(n1.quic_addr, cluster_key)
    n2.wait_healthy()

    time.sleep(3)

    for node in [n1, n2]:
        resp = requests.get(f"{node.url}/cluster/health/reader", timeout=5)
        assert resp.status_code == 200, (
            f"Node {node.node_id} /cluster/health/reader should be 200, "
            f"got {resp.status_code}: {resp.text}"
        )
        body = resp.json()
        assert body["status"] == "available"
        assert body["capability"] == "reader"
        assert body["role"] in ("leader", "follower")


def test_health_endpoints_no_auth_required(node_factory):
    """Health endpoints should work without Authorization header."""
    n1 = node_factory(node_id=1)
    cluster_key = n1.start_create()
    n1.wait_healthy()

    n2 = node_factory(node_id=2)
    n2.start_join(n1.quic_addr, cluster_key)
    n2.wait_healthy()

    time.sleep(3)

    # No auth headers — should still get 200 (not 401)
    for node in [n1, n2]:
        resp = requests.get(f"{node.url}/cluster/health/writer", timeout=5)
        assert resp.status_code in (200, 503), (
            f"Node {node.node_id} /cluster/health/writer without auth: "
            f"expected 200 or 503, got {resp.status_code}: {resp.text}"
        )

        resp = requests.get(f"{node.url}/cluster/health/reader", timeout=5)
        assert resp.status_code == 200, (
            f"Node {node.node_id} /cluster/health/reader without auth: "
            f"expected 200, got {resp.status_code}: {resp.text}"
        )


def test_health_writer_single_node(node_factory):
    """Single-node cluster should report writer available (it's the leader)."""
    n1 = node_factory(node_id=1)
    n1.start_create()
    n1.wait_healthy()

    time.sleep(2)

    resp = requests.get(f"{n1.url}/cluster/health/writer", timeout=5)
    assert resp.status_code == 200, (
        f"Single-node /cluster/health/writer should be 200, got {resp.status_code}"
    )
    body = resp.json()
    assert body["status"] == "available"
    assert body["role"] == "leader"
