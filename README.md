# RediStomp

RediStomp is a simple Websocket wrapper around Redis Pub/Sub to allow delivery of messages via STOMP protocol

## Running the tests

The test suite is hermetic — it needs no real Redis and makes no external
network calls (Redis is faked with `fakeredis`, and connection-failure paths use
a local throwaway socket server).

```bash
pip install -r requirements.txt -r test-requirements.txt
pytest
```

Notes:
- `requirements.txt` pins `uvloop`, which is Linux/macOS only. On Windows,
  install the two dependency files with `uvloop` filtered out
  (nothing in the test suite needs it); CI runs the full suite on Linux.
- The suite runs on every pull request via `.github/workflows/pr_test.yaml`.
