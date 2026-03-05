"""
Test 16: Network partition simulation — verify Raft split-brain handling.

Since we cannot use iptables/firewall rules without root, we simulate
network partitions by killing nodes (minority partition) and verifying
the majority continues to operate correctly. This tests the same Raft
invariants:

  - A majority partition can elect a leader
  - A majority partition can accept and commit writes
  - A minority partition (killed nodes) cannot disrupt the majority
  - After the partition heals (nodes restart), the minority catches up
  - All nodes converge to the same state

Scenarios:
  1. 3-node cluster: kill 1 node (minority), verify 2-node majority works,
     restart the killed node, verify convergence.
  2. 5-node cluster: kill 2 nodes (minority), verify 3-node majority works,
     restart both killed nodes, verify convergence.
"""

import time


def _wait_for_leader(nodes, excluded_leader_id=None, timeout=30):
    """Wait for a leader to be elected among the given nodes.

    If excluded_leader_id is set, wait for a *different* leader.
    Returns (leader_id, leader_node).
    """
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


# ---------------------------------------------------------------------------
# Test 1: 3-node cluster, kill 1 (minority), verify majority, heal, converge
# ---------------------------------------------------------------------------


def test_partition_3_node_kill_minority(node_factory):
    """Simulate a network partition in a 3-node cluster by killing one node.

    The 2-node majority should continue operating (leader election + writes).
    After the killed node restarts, it should catch up to the majority's state.

    This verifies the core Raft invariant: a majority quorum (2 of 3) can
    make progress while the minority (1 of 3) is isolated.
    """
    # --- Phase 1: Create a healthy 3-node cluster ---
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

    # Verify all three nodes are voters
    status = n1.get_status()
    voters = status.get("voters", [])
    assert len(voters) == 3, f"Expected 3 voters, got {voters}"

    # --- Phase 2: Write initial data to verify cluster works ---
    initial_docs = [
        {"id": i, "title": f"Pre-partition doc {i}", "content": f"Written before the split {i}"}
        for i in range(20)
    ]
    task_uid = n1.add_documents("partition-test", initial_docs)
    result = n1.wait_task(task_uid)
    assert result["status"] == "succeeded", f"Initial write failed: {result}"

    _wait_for_doc_count([n1, n2, n3], "partition-test", 20, label="Pre-partition")

    # --- Phase 3: Kill one node to simulate minority partition ---
    # Identify the current leader so we know what to expect
    status = n1.get_status()
    leader_id = status["raftLeaderId"]
    nodes = {1: n1, 2: n2, 3: n3}

    # Kill a non-leader node (the "partitioned" minority)
    # Pick a follower so we don't force a re-election, keeping the test simpler
    follower_ids = [nid for nid in [1, 2, 3] if nid != leader_id]
    partitioned_id = follower_ids[0]
    partitioned_node = nodes[partitioned_id]
    majority_nodes = [n for nid, n in nodes.items() if nid != partitioned_id]

    partitioned_node.kill()

    time.sleep(2)  # let the cluster detect the dead node

    # --- Phase 4: Verify majority can still elect a leader and accept writes ---
    leader_id_after, _ = _wait_for_leader(majority_nodes, timeout=15)
    assert leader_id_after is not None, "Majority lost its leader after partition"

    # Write new documents to the majority during the partition
    partition_docs = [
        {"id": 20 + i, "title": f"During-partition doc {i}", "content": f"Written during split {i}"}
        for i in range(30)
    ]
    leader_node = nodes[leader_id_after]
    task_uid = leader_node.add_documents("partition-test", partition_docs)
    result = leader_node.wait_task(task_uid, timeout=30)
    assert result["status"] == "succeeded", f"Write during partition failed: {result}"

    # Verify the majority has 50 documents (20 initial + 30 during partition)
    _wait_for_doc_count(majority_nodes, "partition-test", 50, label="During partition")

    # Verify reads work on majority nodes
    for node in majority_nodes:
        results = node.search("partition-test", "Pre-partition")
        assert results["estimatedTotalHits"] >= 1, (
            f"Node {node.node_id}: pre-partition data missing during partition"
        )
        results = node.search("partition-test", "During-partition")
        assert results["estimatedTotalHits"] >= 1, (
            f"Node {node.node_id}: during-partition data not searchable"
        )

    # Write even more data to stress the catch-up later
    more_docs = [
        {"id": 50 + i, "title": f"Extra partition doc {i}", "content": f"Extra data {i}"}
        for i in range(20)
    ]
    task_uid = leader_node.add_documents("partition-test", more_docs)
    result = leader_node.wait_task(task_uid, timeout=30)
    assert result["status"] == "succeeded", f"Extra write during partition failed: {result}"

    _wait_for_doc_count(majority_nodes, "partition-test", 70, label="Extra writes")

    # --- Phase 5: Heal the partition (restart the killed node) ---
    partitioned_node.start_restart()
    partitioned_node.wait_healthy(timeout=30)

    # --- Phase 6: Verify convergence — the restarted node catches up ---
    _wait_for_doc_count(
        [partitioned_node], "partition-test", 70,
        timeout=45, label="Post-heal catch-up"
    )

    # Verify search works on the healed node
    results = partitioned_node.search("partition-test", "Pre-partition")
    assert results["estimatedTotalHits"] >= 1, (
        "Healed node missing pre-partition data"
    )
    results = partitioned_node.search("partition-test", "During-partition")
    assert results["estimatedTotalHits"] >= 1, (
        "Healed node missing during-partition data"
    )
    results = partitioned_node.search("partition-test", "Extra partition")
    assert results["estimatedTotalHits"] >= 1, (
        "Healed node missing extra partition data"
    )

    # --- Phase 7: Verify the full cluster can still accept writes after healing ---
    post_heal_docs = [
        {"id": 70 + i, "title": f"Post-heal doc {i}", "content": f"After reunion {i}"}
        for i in range(10)
    ]
    task_uid = leader_node.add_documents("partition-test", post_heal_docs)
    result = leader_node.wait_task(task_uid, timeout=30)
    assert result["status"] == "succeeded", f"Post-heal write failed: {result}"

    # All 3 nodes should converge to 80 documents
    _wait_for_doc_count([n1, n2, n3], "partition-test", 80, timeout=30, label="Post-heal")


# ---------------------------------------------------------------------------
# Test 2: 3-node cluster, kill the leader (minority of 1), verify majority
#          elects new leader, writes succeed, killed leader restarts and
#          catches up.
# ---------------------------------------------------------------------------


def test_partition_leader_isolated(node_factory):
    """Simulate the leader being isolated (killed) from the cluster.

    This is the harder partition case: the leader itself is partitioned away.
    The remaining 2 nodes must detect the leader is gone, elect a new leader,
    and continue serving reads and writes. When the old leader rejoins, it
    must step down and catch up as a follower.
    """
    # --- Create 3-node cluster ---
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

    # Write initial data
    docs = [
        {"id": i, "title": f"Leader-partition doc {i}"}
        for i in range(15)
    ]
    task_uid = n1.add_documents("leader-part", docs)
    result = n1.wait_task(task_uid)
    assert result["status"] == "succeeded", f"Initial write failed: {result}"

    _wait_for_doc_count([n1, n2, n3], "leader-part", 15, label="Pre-leader-kill")

    # Identify the leader and kill it
    status = n1.get_status()
    old_leader_id = status["raftLeaderId"]
    nodes = {1: n1, 2: n2, 3: n3}
    old_leader = nodes[old_leader_id]
    survivors = [n for nid, n in nodes.items() if nid != old_leader_id]

    old_leader.kill()

    # Wait for a new leader among survivors
    new_leader_id, _ = _wait_for_leader(
        survivors, excluded_leader_id=old_leader_id, timeout=30
    )
    assert new_leader_id != old_leader_id, "New leader should differ from killed leader"
    new_leader = nodes[new_leader_id]

    # Write documents via the new leader during the partition
    partition_docs = [
        {"id": 15 + i, "title": f"New-leader doc {i}"}
        for i in range(25)
    ]
    task_uid = new_leader.add_documents("leader-part", partition_docs)
    result = new_leader.wait_task(task_uid, timeout=30)
    assert result["status"] == "succeeded", f"Write via new leader failed: {result}"

    _wait_for_doc_count(survivors, "leader-part", 40, label="During leader partition")

    # Restart the old leader (it should rejoin as a follower and catch up)
    old_leader.start_restart()
    old_leader.wait_healthy(timeout=30)

    # Verify the old leader catches up to the full state
    _wait_for_doc_count(
        [old_leader], "leader-part", 40,
        timeout=45, label="Old leader catch-up"
    )

    # Verify the old leader can serve reads correctly
    results = old_leader.search("leader-part", "Leader-partition")
    assert results["estimatedTotalHits"] >= 1, (
        "Restarted old leader missing pre-partition data"
    )
    results = old_leader.search("leader-part", "New-leader")
    assert results["estimatedTotalHits"] >= 1, (
        "Restarted old leader missing data written by new leader"
    )

    # Final write to confirm the full cluster works
    final_docs = [{"id": 100, "title": "Final convergence doc"}]
    task_uid = new_leader.add_documents("leader-part", final_docs)
    result = new_leader.wait_task(task_uid, timeout=30)
    assert result["status"] == "succeeded", f"Final write failed: {result}"

    _wait_for_doc_count([n1, n2, n3], "leader-part", 41, timeout=30, label="Final convergence")


# ---------------------------------------------------------------------------
# Test 3: 5-node cluster, kill 2 (minority), verify 3-node majority works,
#          restart the 2 killed nodes, verify convergence.
# ---------------------------------------------------------------------------


def test_partition_5_node_kill_minority(node_factory):
    """Simulate a network partition in a 5-node cluster by killing two nodes.

    With 5 nodes, quorum requires 3. Killing 2 leaves a 3-node majority
    that should continue operating. After restarting the 2 killed nodes,
    all 5 should converge.

    This is a more realistic partition simulation: 2 nodes are isolated
    (minority) while 3 nodes continue (majority).
    """
    # --- Create 5-node cluster ---
    n1 = node_factory(node_id=1)
    cluster_key = n1.start_create()
    n1.wait_healthy()

    joiners = []
    for nid in [2, 3, 4, 5]:
        node = node_factory(node_id=nid)
        node.start_join(n1.quic_addr, cluster_key)
        node.wait_healthy()
        joiners.append(node)
        time.sleep(1)  # stagger joins slightly

    n2, n3, n4, n5 = joiners
    all_nodes = {1: n1, 2: n2, 3: n3, 4: n4, 5: n5}

    time.sleep(5)  # extra stabilization for 5-node cluster

    # Verify all 5 nodes are voters
    status = n1.get_status()
    voters = status.get("voters", [])
    assert len(voters) == 5, f"Expected 5 voters, got {voters}"

    # Write initial data
    initial_docs = [
        {"id": i, "title": f"Five-node doc {i}", "data": f"payload {i}"}
        for i in range(30)
    ]
    task_uid = n1.add_documents("five-node", initial_docs)
    result = n1.wait_task(task_uid)
    assert result["status"] == "succeeded", f"Initial write failed: {result}"

    _wait_for_doc_count(
        list(all_nodes.values()), "five-node", 30,
        timeout=30, label="5-node initial"
    )

    # --- Kill 2 non-leader nodes (the minority partition) ---
    status = n1.get_status()
    leader_id = status["raftLeaderId"]

    # Pick 2 non-leader nodes to kill
    non_leader_ids = [nid for nid in all_nodes if nid != leader_id]
    kill_ids = non_leader_ids[:2]
    majority_ids = [nid for nid in all_nodes if nid not in kill_ids]

    for kid in kill_ids:
        all_nodes[kid].kill()

    majority_nodes = [all_nodes[nid] for nid in majority_ids]

    time.sleep(3)  # let cluster detect dead nodes

    # --- Verify majority (3 nodes) still has a leader and can write ---
    leader_id_after, _ = _wait_for_leader(majority_nodes, timeout=15)
    assert leader_id_after is not None, "Majority lost leader after killing 2 nodes"
    leader_node = all_nodes[leader_id_after]

    partition_docs = [
        {"id": 30 + i, "title": f"Majority-only doc {i}", "data": f"during partition {i}"}
        for i in range(40)
    ]
    task_uid = leader_node.add_documents("five-node", partition_docs)
    result = leader_node.wait_task(task_uid, timeout=30)
    assert result["status"] == "succeeded", f"Majority write failed: {result}"

    _wait_for_doc_count(
        majority_nodes, "five-node", 70,
        timeout=30, label="5-node majority"
    )

    # Verify reads on majority
    for node in majority_nodes:
        results = node.search("five-node", "Five-node")
        assert results["estimatedTotalHits"] >= 1, (
            f"Node {node.node_id}: pre-partition data missing"
        )
        results = node.search("five-node", "Majority-only")
        assert results["estimatedTotalHits"] >= 1, (
            f"Node {node.node_id}: partition-time data missing"
        )

    # --- Heal: restart the 2 killed nodes ---
    for kid in kill_ids:
        all_nodes[kid].start_restart()

    for kid in kill_ids:
        all_nodes[kid].wait_healthy(timeout=30)

    # --- Verify convergence: all 5 nodes have 70 documents ---
    healed_nodes = [all_nodes[kid] for kid in kill_ids]
    _wait_for_doc_count(
        healed_nodes, "five-node", 70,
        timeout=60, label="5-node healed catch-up"
    )

    # Verify the healed nodes can serve correct search results
    for kid in kill_ids:
        node = all_nodes[kid]
        results = node.search("five-node", "Five-node")
        assert results["estimatedTotalHits"] >= 1, (
            f"Healed node {kid}: pre-partition data missing"
        )
        results = node.search("five-node", "Majority-only")
        assert results["estimatedTotalHits"] >= 1, (
            f"Healed node {kid}: partition-time data missing after catch-up"
        )

    # --- Final write to confirm full cluster health ---
    post_heal_docs = [
        {"id": 70 + i, "title": f"Reunited doc {i}"}
        for i in range(10)
    ]
    task_uid = leader_node.add_documents("five-node", post_heal_docs)
    result = leader_node.wait_task(task_uid, timeout=30)
    assert result["status"] == "succeeded", f"Post-heal write failed: {result}"

    _wait_for_doc_count(
        list(all_nodes.values()), "five-node", 80,
        timeout=30, label="5-node final convergence"
    )
