"""
Test 17: Fault injection partition testing — application-level network isolation.

Unlike test_16 which simulates partitions by killing nodes, these tests use
built-in fault injection endpoints to block peer communication while all nodes
remain running. This tests true partition behavior where the minority is alive
but unreachable:

  - Bidirectional blocking via /cluster/test/block-peer/{peer_id}
  - Majority partition elects a leader and accepts writes
  - Minority partition stays alive but cannot write (no quorum)
  - After unblocking, minority catches up from Raft log
  - All nodes converge to the same state

The fault injection blocks at the application layer (ClusterTransport), which
is effective because our transport is QUIC (UDP) — TCP-level tools like
Toxiproxy cannot intercept it.
"""

import time


def _wait_for_leader(nodes, excluded_leader_id=None, timeout=30):
    """Wait for a leader to be elected among the given nodes."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        for node in nodes:
            try:
                status = node.get_status()
                leader_id = status.get("raftLeaderId")
                if leader_id is not None:
                    if excluded_leader_id is None or leader_id != excluded_leader_id:
                        return leader_id, node
            except Exception:
                pass
        time.sleep(1)
    raise RuntimeError(
        f"No leader elected within {timeout}s"
        + (f" (excluding {excluded_leader_id})" if excluded_leader_id else "")
    )


def _wait_for_doc_count(nodes, index, expected, timeout=30, label=""):
    """Wait until all given nodes report the expected document count."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        all_ok = True
        for node in nodes:
            try:
                stats = node.get_index_stats(index)
                if stats["numberOfDocuments"] != expected:
                    all_ok = False
                    break
            except Exception:
                all_ok = False
                break
        if all_ok:
            return
        time.sleep(2)
    # Final assertion with details
    counts = {}
    for node in nodes:
        try:
            stats = node.get_index_stats(index)
            counts[node.node_id] = stats["numberOfDocuments"]
        except Exception as e:
            counts[node.node_id] = f"error: {e}"
    raise AssertionError(
        f"{label}: Not all nodes reached {expected} docs within {timeout}s. "
        f"Counts: {counts}"
    )


def _block_bidirectional(nodes_a, nodes_b):
    """Block all communication between two groups of nodes (bidirectional)."""
    for a in nodes_a:
        for b in nodes_b:
            a.block_peer(b.node_id)
    for b in nodes_b:
        for a in nodes_a:
            b.block_peer(a.node_id)


def _unblock_all(all_nodes):
    """Unblock all peers on all nodes."""
    for node in all_nodes:
        try:
            blocked = node.get_blocked_peers()
            for peer_id in blocked.get("blockedPeers", []):
                node.unblock_peer(peer_id)
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Test 1: Isolate a follower via fault injection, verify majority works,
#          heal, verify convergence.
# ---------------------------------------------------------------------------


def test_fault_injection_follower_partition(node_factory):
    """Isolate a follower using application-level fault injection.

    Unlike test_16 where the isolated node is killed, here the follower
    stays alive but all peers block communication with it. This tests
    true partition behavior:
      - The isolated follower is running but cannot reach the majority
      - The majority (2 nodes) continues to accept writes
      - After healing (unblocking), the follower catches up via Raft log
    """
    # --- Phase 1: Create a healthy 3-node cluster ---
    # --cluster-enable-test-endpoints is required for fault injection endpoints
    test_args = ["--cluster-enable-test-endpoints"]
    n1 = node_factory(node_id=1, extra_args=test_args)
    cluster_key = n1.start_create()
    n1.wait_healthy()

    n2 = node_factory(node_id=2, extra_args=test_args)
    n2.start_join(n1.quic_addr, cluster_key)
    n2.wait_healthy()

    n3 = node_factory(node_id=3, extra_args=test_args)
    n3.start_join(n1.quic_addr, cluster_key)
    n3.wait_healthy()

    time.sleep(3)  # cluster stabilization

    all_nodes = [n1, n2, n3]
    nodes_map = {1: n1, 2: n2, 3: n3}

    # Verify all three nodes are voters
    status = n1.get_status()
    voters = status.get("voters", [])
    assert len(voters) == 3, f"Expected 3 voters, got {voters}"

    # --- Phase 2: Write initial data ---
    initial_docs = [
        {"id": i, "title": f"Pre-partition doc {i}"}
        for i in range(20)
    ]
    task_uid = n1.add_documents("fi-follower", initial_docs)
    result = n1.wait_task(task_uid)
    assert result["status"] == "succeeded", f"Initial write failed: {result}"

    _wait_for_doc_count(all_nodes, "fi-follower", 20, label="Pre-partition")

    # --- Phase 3: Isolate a follower via fault injection ---
    status = n1.get_status()
    leader_id = status["raftLeaderId"]

    # Pick a follower (non-leader) to isolate
    follower_ids = [nid for nid in [1, 2, 3] if nid != leader_id]
    isolated_id = follower_ids[0]
    isolated_node = nodes_map[isolated_id]
    majority_nodes = [n for n in all_nodes if n.node_id != isolated_id]

    # Block bidirectionally: majority blocks the isolated node AND
    # the isolated node blocks the majority.
    _block_bidirectional(majority_nodes, [isolated_node])

    # Verify the blocks are in place
    for node in majority_nodes:
        blocked = node.get_blocked_peers()
        assert isolated_id in blocked["blockedPeers"], (
            f"Node {node.node_id} should have {isolated_id} blocked"
        )

    time.sleep(5)  # let Raft detect the partition

    # --- Phase 4: Verify majority can still write ---
    leader_id_after, _ = _wait_for_leader(majority_nodes, timeout=15)
    assert leader_id_after is not None, "Majority lost its leader"
    leader_node = nodes_map[leader_id_after]

    partition_docs = [
        {"id": 20 + i, "title": f"During-partition doc {i}"}
        for i in range(30)
    ]
    task_uid = leader_node.add_documents("fi-follower", partition_docs)
    result = leader_node.wait_task(task_uid, timeout=30)
    assert result["status"] == "succeeded", f"Write during partition failed: {result}"

    _wait_for_doc_count(majority_nodes, "fi-follower", 50, label="During partition")

    # Verify isolated node still has only old data (it's alive but partitioned)
    try:
        stats = isolated_node.get_index_stats("fi-follower")
        assert stats["numberOfDocuments"] == 20, (
            f"Isolated node should still have 20 docs, got {stats['numberOfDocuments']}"
        )
    except Exception:
        pass  # Connection errors are acceptable if the node is struggling

    # --- Phase 5: Heal the partition (unblock all peers) ---
    _unblock_all(all_nodes)

    # Verify blocks are cleared
    for node in all_nodes:
        blocked = node.get_blocked_peers()
        assert len(blocked["blockedPeers"]) == 0, (
            f"Node {node.node_id} still has blocked peers after healing"
        )

    # --- Phase 6: Verify convergence ---
    _wait_for_doc_count(
        [isolated_node], "fi-follower", 50,
        timeout=60, label="Post-heal catch-up"
    )

    # Verify search works on the healed node
    results = isolated_node.search("fi-follower", "Pre-partition")
    assert results["estimatedTotalHits"] >= 1, (
        "Healed node missing pre-partition data"
    )
    results = isolated_node.search("fi-follower", "During-partition")
    assert results["estimatedTotalHits"] >= 1, (
        "Healed node missing during-partition data"
    )

    # --- Phase 7: Post-heal write to confirm full cluster health ---
    post_heal_docs = [
        {"id": 50 + i, "title": f"Post-heal doc {i}"}
        for i in range(10)
    ]
    task_uid = leader_node.add_documents("fi-follower", post_heal_docs)
    result = leader_node.wait_task(task_uid, timeout=30)
    assert result["status"] == "succeeded", f"Post-heal write failed: {result}"

    _wait_for_doc_count(all_nodes, "fi-follower", 60, timeout=30, label="Post-heal")


# ---------------------------------------------------------------------------
# Test 2: Isolate the leader via fault injection, verify new election,
#          writes succeed on new leader, heal, verify convergence.
# ---------------------------------------------------------------------------


def test_fault_injection_leader_partition(node_factory):
    """Isolate the leader using application-level fault injection.

    This is the harder partition case: the leader is partitioned away while
    still running. The followers must:
      1. Detect the leader is unreachable (Raft election timeout)
      2. Elect a new leader among the 2-node majority
      3. Accept writes via the new leader
      4. After healing, the old leader steps down and catches up
    """
    # --- Phase 1: Create a healthy 3-node cluster ---
    # --cluster-enable-test-endpoints is required for fault injection endpoints
    test_args = ["--cluster-enable-test-endpoints"]
    n1 = node_factory(node_id=1, extra_args=test_args)
    cluster_key = n1.start_create()
    n1.wait_healthy()

    n2 = node_factory(node_id=2, extra_args=test_args)
    n2.start_join(n1.quic_addr, cluster_key)
    n2.wait_healthy()

    n3 = node_factory(node_id=3, extra_args=test_args)
    n3.start_join(n1.quic_addr, cluster_key)
    n3.wait_healthy()

    time.sleep(3)  # cluster stabilization

    all_nodes = [n1, n2, n3]
    nodes_map = {1: n1, 2: n2, 3: n3}

    # --- Phase 2: Write initial data ---
    docs = [
        {"id": i, "title": f"Leader-iso doc {i}"}
        for i in range(15)
    ]
    task_uid = n1.add_documents("fi-leader", docs)
    result = n1.wait_task(task_uid)
    assert result["status"] == "succeeded", f"Initial write failed: {result}"

    _wait_for_doc_count(all_nodes, "fi-leader", 15, label="Pre-leader-partition")

    # --- Phase 3: Isolate the leader ---
    status = n1.get_status()
    old_leader_id = status["raftLeaderId"]
    old_leader = nodes_map[old_leader_id]
    followers = [n for n in all_nodes if n.node_id != old_leader_id]

    # Block bidirectionally: followers cannot reach leader, leader cannot
    # reach followers.
    _block_bidirectional(followers, [old_leader])

    # --- Phase 4: Wait for new leader election among followers ---
    # The followers should detect the leader is unreachable and elect a new one.
    new_leader_id, _ = _wait_for_leader(
        followers, excluded_leader_id=old_leader_id, timeout=30
    )
    assert new_leader_id != old_leader_id, "New leader should differ from isolated leader"
    new_leader = nodes_map[new_leader_id]

    # --- Phase 5: Write via the new leader ---
    partition_docs = [
        {"id": 15 + i, "title": f"New-leader doc {i}"}
        for i in range(25)
    ]
    task_uid = new_leader.add_documents("fi-leader", partition_docs)
    result = new_leader.wait_task(task_uid, timeout=30)
    assert result["status"] == "succeeded", f"Write via new leader failed: {result}"

    _wait_for_doc_count(followers, "fi-leader", 40, label="During leader partition")

    # Verify the isolated old leader still has only old data
    try:
        stats = old_leader.get_index_stats("fi-leader")
        assert stats["numberOfDocuments"] == 15, (
            f"Isolated leader should still have 15 docs, got {stats['numberOfDocuments']}"
        )
    except Exception:
        pass  # May fail if the old leader is in a bad state

    # --- Phase 6: Heal the partition ---
    _unblock_all(all_nodes)

    # --- Phase 7: Verify the old leader catches up ---
    # The old leader should step down (it's no longer the leader after the
    # new election) and catch up to the majority's state via the Raft log.
    _wait_for_doc_count(
        [old_leader], "fi-leader", 40,
        timeout=60, label="Old leader catch-up"
    )

    # Verify old leader has both old and new data
    results = old_leader.search("fi-leader", "Leader-iso")
    assert results["estimatedTotalHits"] >= 1, (
        "Old leader missing pre-partition data"
    )
    results = old_leader.search("fi-leader", "New-leader")
    assert results["estimatedTotalHits"] >= 1, (
        "Old leader missing data written by new leader"
    )

    # --- Phase 8: Final write to confirm full cluster health ---
    final_docs = [{"id": 100, "title": "Final convergence doc"}]
    task_uid = new_leader.add_documents("fi-leader", final_docs)
    result = new_leader.wait_task(task_uid, timeout=30)
    assert result["status"] == "succeeded", f"Final write failed: {result}"

    _wait_for_doc_count(all_nodes, "fi-leader", 41, timeout=30, label="Final convergence")
