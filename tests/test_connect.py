"""Tests for redis_stomp.redis_connector.connect / aio_connect.

These assert the clients are built with the right parameters. No real Redis is
contacted -- redis-py builds connection pools lazily, and the cluster path is
stubbed so nothing tries to discover slots.
"""
import socket

import pytest
import redis
import redis.asyncio

import redis_stomp.redis_connector as rc
from redis_stomp.redis_connector import connect, aio_connect, HeadlessSentinelSync, HeadlessSentinel


def _kwargs(client):
    return client.connection_pool.connection_kwargs


# --- sync single instance --------------------------------------------------

def test_connect_single_kwargs():
    client = connect("redis://:secret@myhost:6380/3")
    assert isinstance(client, redis.Redis)
    ck = _kwargs(client)
    assert ck["host"] == "myhost"
    assert ck["port"] == 6380
    assert ck["db"] == 3
    assert ck["password"] == "secret"
    assert ck["health_check_interval"] == rc.REDIS_HEALTH_CHECK_INTERVAL == 3
    assert ck["client_name"] == rc.CLIENT_NAME
    assert isinstance(ck["retry"], redis.retry.Retry)


def test_connect_socket_timeout_override():
    assert _kwargs(connect("redis://h:6379", socket_timeout=7))["socket_timeout"] == 7


def test_connect_socket_timeout_from_url():
    assert _kwargs(connect("redis://h:6379?socket_timeout=4"))["socket_timeout"] == 4.0


def test_connect_decode_responses_flag():
    assert _kwargs(connect("redis://h:6379", decode_responses=True))["decode_responses"] is True


# --- sync sentinel ---------------------------------------------------------

def test_connect_sentinel_master():
    client = connect("redis+sentinel://h1:26379,h2:26379/mymaster/0")
    assert type(client.connection_pool).__name__ == "SentinelConnectionPool"
    assert client.connection_pool.is_master is True


def test_connect_sentinel_slave():
    client = connect("redis+sentinel://h1:26379,h2:26379/mymaster/0", read_only=True)
    assert client.connection_pool.is_master is False


# --- sync cluster (stubbed so it doesn't try to reach a cluster) -----------

def test_connect_cluster_kwargs(monkeypatch):
    captured = {}

    def fake_cluster(**kwargs):
        captured.update(kwargs)
        return "CLUSTER"

    monkeypatch.setattr(redis, "RedisCluster", fake_cluster)
    result = connect("redis-cluster://chost:7000")
    assert result == "CLUSTER"
    assert captured["host"] == "chost"
    assert captured["port"] == 7000
    assert captured["health_check_interval"] == 3
    assert captured["client_name"] == rc.CLIENT_NAME
    assert isinstance(captured["retry"], redis.retry.Retry)


# --- async single instance -------------------------------------------------

def test_aio_connect_single_kwargs():
    client = aio_connect("redis://ahost:6379/1")
    assert isinstance(client, redis.asyncio.Redis)
    ck = _kwargs(client)
    assert ck["host"] == "ahost"
    assert ck["db"] == 1
    assert ck["client_name"] == rc.CLIENT_NAME
    assert isinstance(ck["retry"], redis.asyncio.retry.Retry)


def test_aio_connect_single_omits_health_check_interval():
    # Documents a real asymmetry: the async single-instance branch does NOT set
    # health_check_interval, unlike the sync branch. If this is ever "fixed" to
    # match, update this test.
    ck = _kwargs(aio_connect("redis://ahost:6379/0"))
    assert ck.get("health_check_interval", 0) == 0


# --- async cluster (stubbed) ----------------------------------------------

def test_aio_connect_cluster_updates_supported_errors(monkeypatch):
    captured = {}

    def fake_cluster(**kwargs):
        captured.update(kwargs)
        return "AIOCLUSTER"

    monkeypatch.setattr(rc, "RedisCluster", fake_cluster)
    result = aio_connect("redis-cluster://chost:7000")
    assert result == "AIOCLUSTER"
    assert captured["host"] == "chost"
    assert captured["client_name"] == rc.CLIENT_NAME
    retry = captured["retry"]
    # aio_connect calls retry.update_supported_errors(DEFAULT_RETRY_ERRORS)
    assert redis.ConnectionError in retry._supported_errors


# --- headless sentinel DNS expansion --------------------------------------

def test_headless_sentinel_sync_expands_dns(monkeypatch):
    monkeypatch.setattr(
        socket, "gethostbyname_ex",
        lambda host: (host, [], ["10.0.0.1", "10.0.0.2", "10.0.0.3"]),
    )
    hs = HeadlessSentinelSync("sentinel-svc", 26379, sentinel_kwargs={})
    assert hs.headless_host == "sentinel-svc"
    assert len(hs.sentinels) == 3


def test_headless_sentinel_from_headless_host(monkeypatch):
    monkeypatch.setattr(
        socket, "gethostbyname_ex",
        lambda host: (host, [], ["10.0.0.1", "10.0.0.2"]),
    )
    hs = HeadlessSentinelSync("svc", 26379, sentinel_kwargs={})
    assert hs.sentinels_from_headless_host() == ["10.0.0.1", "10.0.0.2"]
