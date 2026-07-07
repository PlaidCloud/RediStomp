"""Tests for redis_stomp.redis_connector.parse_url."""
import pytest

from redis_stomp.redis_connector import parse_url


# --- single instance -------------------------------------------------------

def test_single_basic():
    r = parse_url("redis://localhost:6379")
    assert r.hosts == [("localhost", 6379)]
    assert r.sentinel is False
    assert r.cluster is False
    assert r.headless is False
    assert r.master is True
    assert r.service_name is None
    assert r.database == 0
    assert r.password is None
    assert r.socket_timeout == 1  # default when not supplied


def test_single_default_port():
    r = parse_url("redis://localhost")
    assert r.hosts == [("localhost", 6379)]


def test_single_db_from_path():
    assert parse_url("redis://localhost:6379/2").database == 2


def test_multiple_hosts():
    r = parse_url("redis://h1:6379,h2:6380")
    assert r.hosts == [("h1", 6379), ("h2", 6380)]


@pytest.mark.parametrize(
    "url,expected",
    [
        ("redis://user:pass@localhost:6379", "pass"),   # user:password
        ("redis://:pass@localhost:6379", "pass"),       # password only, leading colon
        ("redis://pass@localhost:6379", "pass"),        # bare token treated as password
        ("redis://localhost:6379", None),               # no auth
    ],
)
def test_password_parsing(url, expected):
    assert parse_url(url).password == expected


# --- query-string options --------------------------------------------------

def test_socket_timeout_query():
    assert parse_url("redis://localhost:6379?socket_timeout=5").socket_timeout == 5.0


def test_socket_timeout_last_value_wins():
    r = parse_url("redis://localhost:6379?socket_timeout=1&socket_timeout=9")
    assert r.socket_timeout == 9.0


def test_quorum_query():
    assert parse_url("redis://localhost:6379?quorum=2").quorum == 2


def test_client_type_slave():
    assert parse_url("redis://localhost:6379?client_type=slave").master is False


def test_client_type_master_default():
    assert parse_url("redis://localhost:6379").master is True


def test_invalid_client_type_raises():
    with pytest.raises(ValueError):
        parse_url("redis://localhost:6379?client_type=bogus")


# --- sentinel --------------------------------------------------------------

def test_sentinel_basic():
    r = parse_url("redis+sentinel://h:26379/mymaster/0")
    assert r.sentinel is True
    assert r.service_name == "mymaster"
    assert r.hosts == [("h", 26379)]
    assert r.database == 0


def test_sentinel_default_port():
    r = parse_url("redis+sentinel://h/mymaster/0")
    assert r.hosts == [("h", 26379)]  # sentinel default port


def test_sentinel_db_from_path():
    assert parse_url("redis+sentinel://h:26379/mymaster/3").database == 3


def test_sentinel_service_name_from_query():
    r = parse_url("redis+sentinel://h:26379?service_name=mymaster")
    assert r.service_name == "mymaster"


@pytest.mark.parametrize(
    "scheme", ["redis+sentinel", "sentinel", "sentinel+headless"]
)
def test_sentinel_scheme_variants(scheme):
    r = parse_url(f"{scheme}://h:26379/mymaster/0")
    assert r.sentinel is True


def test_headless_flag():
    r = parse_url("sentinel+headless://h:26379/mymaster/0")
    assert r.headless is True
    assert r.sentinel is True


def test_sentinel_without_service_name_raises():
    with pytest.raises(ValueError):
        parse_url("redis+sentinel://h:26379")


def test_sentinel_single_path_part_raises():
    # Only "/mymaster" -> no service name is derived (needs 2 path parts) -> error
    with pytest.raises(ValueError):
        parse_url("redis+sentinel://h:26379/mymaster")


# --- cluster ---------------------------------------------------------------

def test_cluster():
    r = parse_url("redis-cluster://h:6379")
    assert r.cluster is True
    assert r.sentinel is False
    assert r.hosts == [("h", 6379)]


# --- scheme validation -----------------------------------------------------

@pytest.mark.parametrize("url", ["http://h:80", "rediss://h:6379", "amqp://h"])
def test_unsupported_scheme_raises(url):
    with pytest.raises(ValueError):
        parse_url(url)


# --- known quirk: ?db= query param is parsed but never applied -------------

@pytest.mark.xfail(reason="db query-string param is parsed into options but never "
                          "used for `database` (only the URL path sets the db); "
                          "see redis_connector.parse_url", strict=True)
def test_db_query_param_should_be_applied():
    assert parse_url("redis://localhost:6379?db=4").database == 4
