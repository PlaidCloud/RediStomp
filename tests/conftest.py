"""Shared fixtures for the RediStomp test suite.

The tests are hermetic: no real Redis and no external network are required.
Redis is either faked with ``fakeredis`` or replaced by a local TCP server that
mimics an unreachable/broken backend.
"""
import os
import socket
import sys
import threading

import pytest

# Make the ``redis_stomp`` package importable when running from the repo root.
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


@pytest.fixture
def fake_redis(monkeypatch):
    """Patch ``connect`` in main + topic modules to hand back fakeredis clients.

    Returns the shared ``FakeServer`` so a test can build its own publisher
    client that talks to the same in-memory instance.
    """
    import fakeredis
    import redis_stomp.main as main_mod
    import redis_stomp.pubsub.topic as topic_mod

    server = fakeredis.FakeServer()

    def fake_connect(redis_url, *args, decode_responses=False, **kwargs):
        return fakeredis.FakeStrictRedis(server=server, decode_responses=decode_responses)

    monkeypatch.setattr(main_mod, "connect", fake_connect)
    monkeypatch.setattr(topic_mod, "connect", fake_connect)
    return server


@pytest.fixture
def accept_close_server():
    """A TCP server that accepts a connection then immediately closes it.

    This is the exact condition that drove the redis-py 5.2.1 recursion bug:
    the socket connects, but every read fails at once. Yields ``(host, port)``.
    """
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("127.0.0.1", 0))
    srv.listen(50)
    stop = threading.Event()

    def serve():
        while not stop.is_set():
            srv.settimeout(0.5)
            try:
                conn, _ = srv.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            try:
                conn.close()
            except OSError:
                pass

    thread = threading.Thread(target=serve, daemon=True)
    thread.start()
    host, port = srv.getsockname()
    try:
        yield host, port
    finally:
        stop.set()
        try:
            srv.close()
        except OSError:
            pass
        thread.join(timeout=2)


@pytest.fixture
def dead_port():
    """Return a port with nothing listening on it (connections are refused)."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port
