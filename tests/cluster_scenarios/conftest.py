"""
Pytest fixtures for Meilisearch cluster scenario tests.

Manages building the binary, starting/stopping meilisearch processes,
and cleaning up data directories.
"""

import json
import os
import shutil
import signal
import socket
import subprocess
import tempfile
import time

import pytest
import requests


BINARY_PATH = os.environ.get("MEILI_BINARY")
MASTER_KEY = "test-master-key-for-cluster-scenarios"
BUILD_TIMEOUT = 600  # 10 minutes for cargo build


def find_free_port():
    """Find a free TCP port on localhost."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def find_free_udp_port():
    """Find a free UDP port on localhost (for QUIC)."""
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture(scope="session")
def binary_path():
    """Build the meilisearch binary with cluster feature, or use MEILI_BINARY env."""
    if BINARY_PATH and os.path.isfile(BINARY_PATH):
        return BINARY_PATH

    # Build from source
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    result = subprocess.run(
        ["cargo", "build", "--features", "cluster", "-p", "meilisearch"],
        cwd=project_root,
        capture_output=True,
        text=True,
        timeout=BUILD_TIMEOUT,
    )
    if result.returncode != 0:
        pytest.skip(f"cargo build failed:\n{result.stderr}")

    path = os.path.join(project_root, "target", "debug", "meilisearch")
    if not os.path.isfile(path):
        pytest.skip(f"Binary not found at {path}")
    return path


class MeilisearchNode:
    """A running meilisearch process."""

    def __init__(self, binary, http_port, quic_port, data_dir, node_id,
                 master_key=None, extra_args=None):
        self.binary = binary
        self.http_port = http_port
        self.quic_port = quic_port
        self.data_dir = data_dir
        self.node_id = node_id
        self.master_key = master_key or MASTER_KEY
        self.url = f"http://127.0.0.1:{http_port}"
        self.quic_addr = f"127.0.0.1:{quic_port}"
        self.process = None
        self.extra_args = extra_args or []

    @property
    def headers(self):
        return {"Authorization": f"Bearer {self.master_key}"}

    def _request(self, method, path, **kwargs):
        """HTTP request with 307 redirect handling that preserves auth.

        Python requests strips the Authorization header on cross-host/port
        redirects (RFC 7235). This helper catches 307 Temporary Redirect
        responses and re-sends the request to the Location with full headers.
        """
        url = f"{self.url}{path}"
        resp = getattr(requests, method)(
            url, allow_redirects=False, **kwargs,
        )
        if resp.status_code == 307 and "Location" in resp.headers:
            location = resp.headers["Location"]
            resp = getattr(requests, method)(location, **kwargs)
        return resp

    def start_create(self):
        """Start as cluster creator (first node)."""
        cmd = [
            self.binary,
            "--db-path", self.data_dir,
            "--http-addr", f"127.0.0.1:{self.http_port}",
            "--master-key", self.master_key,
            "--env", "development",
            "--cluster-create",
            "--cluster-bind", f"127.0.0.1:{self.quic_port}",
            "--cluster-node-id", str(self.node_id),
            "--no-analytics",
        ] + self.extra_args
        self._start(cmd)
        return self._read_cluster_key()

    def start_join(self, bootstrap_addr, cluster_secret):
        """Start as a joiner to an existing cluster."""
        cmd = [
            self.binary,
            "--db-path", self.data_dir,
            "--http-addr", f"127.0.0.1:{self.http_port}",
            "--master-key", self.master_key,
            "--env", "development",
            "--cluster-join", bootstrap_addr,
            "--cluster-secret", cluster_secret,
            "--cluster-bind", f"127.0.0.1:{self.quic_port}",
            "--cluster-node-id", str(self.node_id),
            "--no-analytics",
        ] + self.extra_args
        self._start(cmd)

    def start_restart(self):
        """Start with just --db-path (auto-restart from persisted state)."""
        cmd = [
            self.binary,
            "--db-path", self.data_dir,
            "--http-addr", f"127.0.0.1:{self.http_port}",
            "--master-key", self.master_key,
            "--env", "development",
            "--no-analytics",
        ] + self.extra_args
        self._start(cmd)

    def _start(self, cmd):
        # Write stderr to a log file instead of a pipe. Pipes have a finite
        # buffer (~64KB on macOS); if nothing reads the pipe and the process
        # writes more than the buffer holds, the process blocks on its next
        # log write, stalling the QUIC accept loop and causing join failures.
        # Log file must live OUTSIDE the data_dir because Meilisearch uses
        # data_dir as --db-path: any extra file there makes is_empty_db() return
        # false, causing "failed to infer the version of the database" errors.
        parent = os.path.dirname(self.data_dir)
        self.log_path = os.path.join(parent, f"node-{self.node_id}-stderr.log")
        self._log_file = open(self.log_path, "w")
        self.process = subprocess.Popen(
            cmd,
            stdout=subprocess.DEVNULL,
            stderr=self._log_file,
        )

    def _read_stderr_log(self):
        """Read the current contents of the stderr log file."""
        self._log_file.flush()
        try:
            with open(self.log_path) as f:
                return f.read()
        except FileNotFoundError:
            return ""

    def _read_cluster_key(self):
        """Read the cluster key from stderr log file."""
        deadline = time.time() + 30
        while time.time() < deadline:
            if self.process.poll() is not None:
                raise RuntimeError(f"Process exited early:\n{self._read_stderr_log()}")
            log = self._read_stderr_log()
            for line in log.splitlines():
                if "Cluster Key:" in line:
                    return line.split("Cluster Key:")[1].strip().split()[0]
            time.sleep(0.2)
        raise RuntimeError(
            f"Timed out waiting for cluster key. Output:\n{self._read_stderr_log()}"
        )

    def wait_healthy(self, timeout=30):
        """Wait for /health to return 200."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            if self.process.poll() is not None:
                raise RuntimeError(f"Process exited:\n{self._read_stderr_log()}")
            try:
                resp = requests.get(f"{self.url}/health", timeout=2)
                if resp.status_code == 200:
                    return
            except requests.ConnectionError:
                pass
            time.sleep(0.5)
        raise RuntimeError(f"Node {self.node_id} did not become healthy within {timeout}s")

    def get_status(self):
        """GET /cluster/status."""
        resp = requests.get(f"{self.url}/cluster/status", headers=self.headers, timeout=5)
        resp.raise_for_status()
        return resp.json()

    def add_documents(self, index, documents):
        """POST documents to an index, return task uid."""
        resp = self._request(
            "post",
            f"/indexes/{index}/documents",
            headers={**self.headers, "Content-Type": "application/json"},
            json=documents,
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json()["taskUid"]

    def wait_task(self, task_uid, timeout=30):
        """Wait for a task to complete (succeeded or failed).

        Handles 404 gracefully since followers may not have replicated the task yet.
        """
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                resp = self._request(
                    "get",
                    f"/tasks/{task_uid}",
                    headers=self.headers,
                    timeout=5,
                )
                if resp.status_code == 404:
                    # Task not yet replicated to this node; retry
                    time.sleep(0.5)
                    continue
                resp.raise_for_status()
                status = resp.json()["status"]
                if status in ("succeeded", "failed"):
                    return resp.json()
            except requests.exceptions.ConnectionError:
                pass
            time.sleep(0.5)
        raise RuntimeError(f"Task {task_uid} did not complete within {timeout}s")

    def search(self, index, query="", **kwargs):
        """POST a search query."""
        body = {"q": query, **kwargs}
        resp = self._request(
            "post",
            f"/indexes/{index}/search",
            headers={**self.headers, "Content-Type": "application/json"},
            json=body,
            timeout=10,
        )
        resp.raise_for_status()
        return resp.json()

    def get_index_stats(self, index):
        """GET /indexes/{index}/stats — returns numberOfDocuments etc."""
        resp = requests.get(
            f"{self.url}/indexes/{index}/stats",
            headers=self.headers,
            timeout=10,
        )
        resp.raise_for_status()
        return resp.json()

    def leave(self, timeout=15):
        """POST /cluster/status/leave."""
        resp = self._request(
            "post",
            "/cluster/status/leave",
            headers=self.headers,
            timeout=timeout,
        )
        return resp

    def stop(self, timeout=10):
        """Stop the process gracefully (SIGTERM), fall back to SIGKILL."""
        if self.process is None or self.process.poll() is not None:
            return
        self.process.send_signal(signal.SIGTERM)
        try:
            self.process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.wait(timeout=5)

    def kill(self):
        """Kill the process immediately (SIGKILL)."""
        if self.process and self.process.poll() is None:
            self.process.kill()
            self.process.wait(timeout=5)

    def block_peer(self, peer_id):
        """Block a peer via fault injection (POST /cluster/test/block-peer/{peer_id})."""
        resp = requests.post(
            f"{self.url}/cluster/test/block-peer/{peer_id}", timeout=5,
        )
        resp.raise_for_status()
        return resp.json()

    def unblock_peer(self, peer_id):
        """Unblock a peer via fault injection (POST /cluster/test/unblock-peer/{peer_id})."""
        resp = requests.post(
            f"{self.url}/cluster/test/unblock-peer/{peer_id}", timeout=5,
        )
        resp.raise_for_status()
        return resp.json()

    def get_blocked_peers(self):
        """Get list of blocked peers (GET /cluster/test/blocked-peers)."""
        resp = requests.get(
            f"{self.url}/cluster/test/blocked-peers", timeout=5,
        )
        resp.raise_for_status()
        return resp.json()


@pytest.fixture
def node_factory(binary_path, tmp_path):
    """Factory fixture that creates MeilisearchNode instances and cleans up."""
    nodes = []

    def _make(node_id=0, master_key=None, extra_args=None):
        http_port = find_free_port()
        quic_port = find_free_udp_port()
        data_dir = str(tmp_path / f"node-{node_id}")
        os.makedirs(data_dir, exist_ok=True)
        node = MeilisearchNode(
            binary=binary_path,
            http_port=http_port,
            quic_port=quic_port,
            data_dir=data_dir,
            node_id=node_id,
            master_key=master_key,
            extra_args=extra_args,
        )
        nodes.append(node)
        return node

    yield _make

    # Cleanup: stop all nodes
    for node in nodes:
        node.stop()
