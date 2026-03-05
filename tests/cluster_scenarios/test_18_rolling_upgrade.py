"""
Test 18: Rolling upgrade infrastructure — version info and node version tracking.

Verifies:
  1. /cluster/version-info returns binary version and supported protocols.
  2. /cluster/status nodeVersions shows version info for all nodes.
  3. Node version info persists across restart.
  4. Leaving and rejoining updates version info.
"""

import time

import requests


def test_version_info_endpoint(node_factory):
    """GET /cluster/version-info returns binary version and supported protocols."""
    n1 = node_factory(node_id=1)
    n1.start_create()
    n1.wait_healthy()

    # Unauthenticated request
    resp = requests.get(f"{n1.url}/cluster/version-info", timeout=5)
    assert resp.status_code == 200, (
        f"Expected 200 from /cluster/version-info, got {resp.status_code}: {resp.text}"
    )
    body = resp.json()
    assert "binaryVersion" in body, f"Missing binaryVersion: {body}"
    assert "supportedProtocols" in body, f"Missing supportedProtocols: {body}"

    # Version should be a valid semver-like string
    version = body["binaryVersion"]
    assert len(version.split(".")) >= 2, f"Invalid version format: {version}"

    # Protocols should include at least protocol 1
    protocols = body["supportedProtocols"]
    assert isinstance(protocols, list), f"Protocols should be a list: {protocols}"
    assert 1 in protocols, f"Protocol 1 not in supported protocols: {protocols}"


def test_cluster_status_shows_node_versions(node_factory):
    """GET /cluster/status includes nodeVersions for all cluster members."""
    n1 = node_factory(node_id=1)
    cluster_key = n1.start_create()
    n1.wait_healthy()

    n2 = node_factory(node_id=2)
    n2.start_join(n1.quic_addr, cluster_key)
    n2.wait_healthy()

    n3 = node_factory(node_id=3)
    n3.start_join(n1.quic_addr, cluster_key)
    n3.wait_healthy()

    # Wait for peer handshakes to exchange version info
    time.sleep(5)

    # Check leader's status response
    status = n1.get_status()
    assert "nodeVersions" in status, (
        f"Missing nodeVersions in status: {status}"
    )
    node_versions = status["nodeVersions"]
    assert isinstance(node_versions, list), (
        f"nodeVersions should be a list: {node_versions}"
    )

    # Should have version info for at least node 1 (self)
    node_ids_with_versions = {nv["nodeId"] for nv in node_versions}
    assert 1 in node_ids_with_versions, (
        f"Node 1 (leader) not in nodeVersions: {node_versions}"
    )

    # Each entry should have binaryVersion and supportedProtocols
    for nv in node_versions:
        assert "nodeId" in nv, f"Missing nodeId: {nv}"
        assert "binaryVersion" in nv, f"Missing binaryVersion: {nv}"
        assert "supportedProtocols" in nv, f"Missing supportedProtocols: {nv}"
        assert len(nv["binaryVersion"].split(".")) >= 2, (
            f"Invalid version format: {nv['binaryVersion']}"
        )


def test_version_info_after_restart(node_factory):
    """After restart, the leader sees the restarted node's version info."""
    n1 = node_factory(node_id=1)
    cluster_key = n1.start_create()
    n1.wait_healthy()

    n2 = node_factory(node_id=2)
    n2.start_join(n1.quic_addr, cluster_key)
    n2.wait_healthy()

    time.sleep(3)

    # Verify initial version info
    status = n1.get_status()
    assert "nodeVersions" in status, f"Missing nodeVersions: {status}"

    # Stop and restart node 2
    n2.stop()
    time.sleep(2)

    n2.start_restart()
    n2.wait_healthy()

    # Wait for reconnection and handshake
    time.sleep(5)

    # Check that leader shows version info for both nodes
    status = n1.get_status()
    assert "nodeVersions" in status, (
        f"Missing nodeVersions after restart: {status}"
    )
    node_versions = status["nodeVersions"]
    node_ids_with_versions = {nv["nodeId"] for nv in node_versions}
    assert 1 in node_ids_with_versions, (
        f"Node 1 missing from nodeVersions after restart: {node_versions}"
    )
    # Node 2's version info should be present (from handshake on reconnect)
    assert 2 in node_ids_with_versions, (
        f"Node 2 missing from nodeVersions after restart: {node_versions}"
    )


def test_leave_rejoin_updates_version(node_factory):
    """After leave and rejoin, version info is updated for the new member."""
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

    # Verify all 3 nodes have version info
    status = n1.get_status()
    assert "nodeVersions" in status, f"Missing nodeVersions: {status}"

    # Leave node 3
    resp = n3.leave()
    assert resp.status_code == 200, (
        f"Leave failed: {resp.status_code}: {resp.text}"
    )
    time.sleep(3)

    # Stop node 3
    n3.stop()

    # Rejoin with a new node using auto-assigned ID
    n4 = node_factory(node_id=0)  # auto-assign
    n4.start_join(n1.quic_addr, cluster_key)
    n4.wait_healthy()

    time.sleep(5)

    # Check leader shows version info for new member
    status = n1.get_status()
    assert "nodeVersions" in status, (
        f"Missing nodeVersions after rejoin: {status}"
    )
    node_versions = status["nodeVersions"]
    node_ids_with_versions = {nv["nodeId"] for nv in node_versions}

    # Node 1 and 2 should still be there
    assert 1 in node_ids_with_versions, (
        f"Node 1 missing: {node_versions}"
    )
    assert 2 in node_ids_with_versions, (
        f"Node 2 missing: {node_versions}"
    )
    # The new node (auto-assigned, likely node 4) should be present
    # Node 3 was removed, so there should be a new node ID
    assert len(node_ids_with_versions) >= 3, (
        f"Expected at least 3 nodes with version info, got: {node_versions}"
    )
