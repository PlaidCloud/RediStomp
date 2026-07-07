"""Tests for the main() entrypoint wiring in redis_stomp.main."""
import sys

import redis_stomp.main as main_mod


def test_main_configures_uvicorn(monkeypatch):
    captured = {}

    def fake_run(app_arg, **kwargs):
        captured["app"] = app_arg
        captured.update(kwargs)

    monkeypatch.setattr(main_mod.uvicorn, "run", fake_run)
    monkeypatch.setattr(
        sys, "argv",
        ["redistomp", "--port", "9999", "--redis", "redis://example:6380/0"],
    )

    main_mod.main()

    assert captured["app"] is main_mod.app
    assert captured["host"] == "0.0.0.0"
    assert captured["port"] == 9999
    assert captured["proxy_headers"] is True
    assert captured["forwarded_allow_ips"] == "*"
    assert captured["access_log"] is False
    # The ws-sansio fix: must use uvicorn's modern websockets implementation
    # so the legacy DeprecationWarnings don't return.
    assert captured["ws"] == "websockets-sansio"
    # The chosen --redis is published on the app for the endpoints to read.
    assert main_mod.app.state.redis_url == "redis://example:6380/0"
