"""Tests for redis_stomp.pubsub.topic.RedisTopicManager.

Uses fakeredis (via the ``fake_redis`` fixture) so pub/sub actually works.
"""
import asyncio

import fakeredis
import pytest

from redis_stomp.pubsub.topic import RedisTopicManager


class StubConnection:
    """Minimal stand-in for a coilmq StompConnection that records sent frames."""

    def __init__(self):
        self.frames = []

    async def send_frame(self, frame):
        self.frames.append(frame)


# --- destination translation (pure) ---------------------------------------

@pytest.mark.parametrize(
    "destination,expected",
    [
        ("/topic/foo", "foo"),
        ("/topic/foo.bar", "foo.bar"),
        ("/topic/a/b", "a/b"),
        ("/topic/a.#", "a.*"),      # '#' becomes '*' (STOMP wildcard -> redis glob)
        ("/topic/a.#.b", "a.*.b"),
        ("not-a-topic", "not-a-topic"),  # non-topic destinations pass through
    ],
)
def test_get_redis_destination(fake_redis, destination, expected):
    tm = RedisTopicManager(StubConnection(), "redis://x")
    assert tm.get_redis_destination(destination) == expected


# --- subscribe / unsubscribe against fakeredis -----------------------------

async def test_subscribe_plain_channel(fake_redis):
    tm = RedisTopicManager(StubConnection(), "redis://x")
    conn = StubConnection()
    await tm.subscribe(conn, "/topic/news", id="sub-1")
    assert "news" in tm._redis.channels
    assert await tm._subscriptions.subscriber_count("news") == 1


async def test_subscribe_wildcard_uses_psubscribe(fake_redis):
    tm = RedisTopicManager(StubConnection(), "redis://x")
    await tm.subscribe(StubConnection(), "/topic/news.*", id="sub-1")
    assert "news.*" in tm._redis.patterns
    assert tm._redis.channels == {}


async def test_subscribe_hash_wildcard(fake_redis):
    tm = RedisTopicManager(StubConnection(), "redis://x")
    await tm.subscribe(StubConnection(), "/topic/a.#", id="sub-1")
    assert "a.*" in tm._redis.patterns


# redis-py only prunes PubSub.channels when the unsubscribe *ack* is read, so we
# assert the observable contract instead: the manager tells redis to unsubscribe
# exactly when the last subscriber for a destination leaves.

async def test_unsubscribe_by_destination_calls_redis(fake_redis, monkeypatch):
    tm = RedisTopicManager(StubConnection(), "redis://x")
    conn = StubConnection()
    await tm.subscribe(conn, "/topic/news", id="sub-1")

    calls = []
    monkeypatch.setattr(tm._redis, "unsubscribe", lambda *a: calls.append(a))
    await tm.unsubscribe(conn, "/topic/news", id="sub-1")

    assert calls == [("news",)]
    assert await tm._subscriptions.subscriber_count("news") == 0


async def test_unsubscribe_by_id_only(fake_redis, monkeypatch):
    tm = RedisTopicManager(StubConnection(), "redis://x")
    conn = StubConnection()
    await tm.subscribe(conn, "/topic/news", id="sub-1")

    calls = []
    monkeypatch.setattr(tm._redis, "unsubscribe", lambda *a: calls.append(a))
    await tm.unsubscribe(conn, destination=None, id="sub-1")

    assert calls == [("news",)]


async def test_unsubscribe_keeps_redis_sub_while_others_remain(fake_redis, monkeypatch):
    tm = RedisTopicManager(StubConnection(), "redis://x")
    c1, c2 = StubConnection(), StubConnection()
    await tm.subscribe(c1, "/topic/news", id="a")
    await tm.subscribe(c2, "/topic/news", id="b")

    calls = []
    monkeypatch.setattr(tm._redis, "unsubscribe", lambda *a: calls.append(a))
    await tm.unsubscribe(c1, "/topic/news", id="a")

    assert calls == []  # c2 is still subscribed, so redis stays subscribed
    assert await tm._subscriptions.subscriber_count("news") == 1


async def test_unsubscribe_wildcard_calls_punsubscribe(fake_redis, monkeypatch):
    tm = RedisTopicManager(StubConnection(), "redis://x")
    conn = StubConnection()
    await tm.subscribe(conn, "/topic/news.*", id="sub-1")

    calls = []
    monkeypatch.setattr(tm._redis, "punsubscribe", lambda *a: calls.append(a))
    await tm.unsubscribe(conn, "/topic/news.*", id="sub-1")

    assert calls == [("news.*",)]


# --- end-to-end message delivery ------------------------------------------

# --- teardown regression: the coroutine-leak bug -------------------------

async def test_next_message_sets_closed_when_cancelled(fake_redis):
    """Regression guard: on disconnect the enclosing task group cancels
    next_message. It must still mark itself closed, otherwise close() waits on
    _closed forever and leaks the coroutine (the original bug)."""
    tm = RedisTopicManager(StubConnection(), "redis://x")
    task = asyncio.create_task(tm.next_message())
    await asyncio.sleep(0.1)  # let it enter the loop

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert tm._closed is True
    # close() must now return promptly rather than spinning on _closed.
    await asyncio.wait_for(tm.close(), timeout=2)


async def test_message_is_delivered_as_stomp_frame(fake_redis):
    from coilmq.util.frames import MESSAGE

    conn = StubConnection()
    tm = RedisTopicManager(conn, "redis://x")
    await tm.subscribe(conn, "/topic/news", id="sub-1")

    publisher = fakeredis.FakeStrictRedis(server=fake_redis, decode_responses=True)

    task = asyncio.create_task(tm.next_message())
    try:
        await asyncio.sleep(0.1)  # let next_message enter its get_message loop
        publisher.publish("news", "hello world")
        for _ in range(50):
            if conn.frames:
                break
            await asyncio.sleep(0.1)
    finally:
        tm._closing = True
        await asyncio.wait_for(task, timeout=3)

    assert len(conn.frames) == 1
    frame = conn.frames[0]
    assert frame.cmd == MESSAGE
    assert frame.body == "hello world"
    assert frame.headers["destination"] == "/topic/news"
    assert frame.headers["subscription"] == "sub-1"
    assert "message-id" in frame.headers
