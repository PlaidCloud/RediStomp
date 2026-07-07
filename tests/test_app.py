"""Tests for the Starlette app in redis_stomp.main: /alive and the /ws bridge."""
import time

import fakeredis
import pytest
import redis
from starlette.testclient import TestClient

from redis_stomp.main import app

NULL = "\x00"


def _frame(command, headers=None, body=""):
    lines = [command] + [f"{k}:{v}" for k, v in (headers or {}).items()]
    return "\n".join(lines) + "\n\n" + body + NULL


def _recv_frame(ws):
    """Receive the next STOMP frame, skipping bare-newline heartbeats."""
    while True:
        data = ws.receive_text()
        if data.strip("\n") == "":
            continue
        return data


def _parse(raw):
    raw = raw.rstrip(NULL)
    head, _, body = raw.partition("\n\n")
    lines = head.split("\n")
    headers = dict(line.split(":", 1) for line in lines[1:] if line)
    return lines[0], headers, body


def test_alive_ok_with_reachable_redis(fake_redis):
    app.state.redis_url = "redis://fake"
    with TestClient(app) as client:
        resp = client.get("/alive")
    assert resp.status_code == 200


def test_alive_returns_500_when_redis_unreachable(dead_port):
    # Real redis_connector.connect() against a refused port -> clean failure -> 500.
    app.state.redis_url = f"redis://127.0.0.1:{dead_port}/0"
    with TestClient(app, raise_server_exceptions=False) as client:
        resp = client.get("/alive")
    assert resp.status_code == 500


def test_alive_does_not_recurse(accept_close_server):
    """Regression guard for the redis-py 5.2.1 health-check recursion.

    Uses the real connect() (health_check_interval + retry + client_name -- the
    exact trigger) against an accept-then-close server. Under redis 7.1.1 this
    must surface a normal error, never RecursionError.
    """
    host, port = accept_close_server
    app.state.redis_url = f"redis://{host}:{port}/0"
    with TestClient(app, raise_server_exceptions=True) as client:
        with pytest.raises(BaseException) as excinfo:
            client.get("/alive")
    assert not isinstance(excinfo.value, RecursionError)
    # and it is a normal redis/connection error
    assert isinstance(excinfo.value, (redis.RedisError, OSError))


@pytest.mark.integration
def test_ws_stomp_connect_returns_connected(fake_redis):
    app.state.redis_url = "redis://fake"
    with TestClient(app) as client:
        with client.websocket_connect("/ws", subprotocols=["v11.stomp"]) as ws:
            ws.send_text(_frame("CONNECT", {"accept-version": "1.1", "host": "localhost"}))
            command, headers, _ = _parse(_recv_frame(ws))
            assert command == "CONNECTED"
            assert headers.get("version") == "1.1"
            ws.close()


@pytest.mark.integration
def test_ws_redis_publish_arrives_as_message_frame(fake_redis):
    """The real bridge: a redis publish to a subscribed topic must reach the
    websocket client as a STOMP MESSAGE frame."""
    app.state.redis_url = "redis://fake"
    with TestClient(app) as client:
        with client.websocket_connect("/ws", subprotocols=["v11.stomp"]) as ws:
            ws.send_text(_frame("CONNECT", {"accept-version": "1.1", "host": "localhost"}))
            assert _parse(_recv_frame(ws))[0] == "CONNECTED"

            ws.send_text(_frame("SUBSCRIBE", {"destination": "/topic/news", "id": "sub-1"}))

            # Publish until the SUBSCRIBE has registered on redis (returns >0 receivers).
            publisher = fakeredis.FakeStrictRedis(server=fake_redis, decode_responses=True)
            for _ in range(100):
                if publisher.publish("news", "hello world") > 0:
                    break
                time.sleep(0.05)
            else:
                pytest.fail("SUBSCRIBE never registered a redis subscriber")

            command, headers, body = _parse(_recv_frame(ws))
            assert command == "MESSAGE"
            assert headers["destination"] == "/topic/news"
            assert headers["subscription"] == "sub-1"
            assert body == "hello world"
            ws.close()
