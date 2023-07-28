
import socket
from typing import List, Tuple, NamedTuple, Type
from urllib import parse as urlparse

from redis.asyncio import Redis, Sentinel

CLIENT_NAME = socket.gethostname().rsplit('-', 2)[0]

class ParsedRedisURL(NamedTuple):
    hosts: List[Tuple[str, int]]
    password: str
    socket_timeout: int
    master: bool
    sentinel: bool
    service_name: str
    database: int = 0
    cluster: bool = False
    # options:


def parse_url(url: str) -> ParsedRedisURL:
    url = urlparse.urlparse(url)

    def is_sentinel():
        return url.scheme == 'redis+sentinel' or url.scheme == 'sentinel'

    def is_cluster():
        return url.scheme == 'redis-cluster'

    if url.scheme != 'redis' and not is_sentinel() and not is_cluster():
        raise ValueError(f'Unsupported scheme: {url.scheme}')

    def parse_host(s: str):
        if ':' in s:
            host, port = s.split(':', 1)
            port = int(port)
        else:
            host = s
            port = 26379 if is_sentinel() else 6379
        return host, port

    if '@' in url.netloc:
        auth, hostspec = url.netloc.split('@', 1)
    else:
        auth = None
        hostspec = url.netloc

    if auth and ':' in auth:
        _, password = auth.split(':', 1)
    elif auth:
        password = auth
    else:
        password = None

    hosts = [parse_host(s) for s in hostspec.split(',')]

    query_string_options = {
        'db': int,
        'service_name': str,
        'client_type': str,
        'socket_timeout': float,
        'socket_connect_timeout': float,
    }
    options = {}

    for name, value in urlparse.parse_qs(url.query).items():
        if name in query_string_options:
            option_type = query_string_options[name]
            # Query string param may be defined multiple times, or with multiple values, so pick the last entry
            options[name] = option_type(value[-1])

    path = url.path
    if path.startswith('/'):
        path = path[1:]
    if path == '':
        path_parts = []
    else:
        # Remove empty strings for non-sentinel paths, like '/0'
        path_parts = [part for part in path.split('/') if part]

    if 'service_name' in options:
        service_name = options.pop('service_name')
    elif len(path_parts) >= 2:
        service_name = path_parts[0]
    else:
        service_name = None

    if is_sentinel() and not service_name:
        raise ValueError('Sentinel URL has no service name specified. Please add it to the URLs path.')

    client_type = options.pop('client_type', 'master')
    if client_type not in ('master', 'slave'):
        raise ValueError('Client type must be either master or slave, got {!r}')

    db = 0
    if 'db' not in options:
        if len(path_parts) >= 2:
            db = int(path_parts[1])
        elif len(path_parts) == 1:
            db = int(path_parts[0])

    return ParsedRedisURL(
        hosts=hosts,
        password=password,
        socket_timeout=options.get("socket_timeout", 1),
        sentinel=is_sentinel(),
        cluster=is_cluster(),
        master=(client_type == "master"),
        service_name=service_name,
        database=db,
    )


def aio_connect(redis_url: str, read_only: bool = False, socket_timeout: float = None,
                redis_class: Type[Redis] = Redis, decode_responses: bool = False):
    """Returns an async Redis connection

    Args:
        redis_url (str): The Redis connection url
        read_only (bool): If true, the connection will be made to the master node, else to a slave node
        socket_timeout (float): Socket timeout threshold
        redis_class (Type[Redis]):
        decode_responses (bool): Redis connection kwarg for decode_responses

    Returns:
        redis.asyncio.client.Redis: Redis connection
    """
    rinfo = parse_url(redis_url)
    if rinfo.sentinel:
        # We're connecting to a sentinel cluster.
        sentinel_connection = Sentinel(
                rinfo.hosts, socket_timeout=socket_timeout or rinfo.socket_timeout,
                db=rinfo.database, password=rinfo.password,
                health_check_interval=30, retry_on_timeout=True,
                client_name=CLIENT_NAME,
        )
        if read_only:
            return sentinel_connection.slave_for(
                rinfo.service_name, socket_timeout=socket_timeout or rinfo.socket_timeout,
                redis_class=redis_class, decode_responses=decode_responses,
                client_name=CLIENT_NAME,
            )
        else:
            return sentinel_connection.master_for(
                rinfo.service_name, socket_timeout=socket_timeout or rinfo.socket_timeout,
                redis_class=redis_class, decode_responses=decode_responses,
                client_name=CLIENT_NAME,
            )
    else:
        # Single redis instance
        host, port = rinfo.hosts[0]
        return redis_class(
            host=host,
            port=port,
            db=rinfo.database,
            password=rinfo.password,
            decode_responses=decode_responses,
            #socket_timeout=socket_timeout or rinfo.socket_timeout,
            health_check_interval=30,
            retry_on_timeout=True,
            client_name=CLIENT_NAME,
        )
