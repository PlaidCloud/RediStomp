
import socket
from typing import List, Tuple, NamedTuple, Type, TypeVar
from urllib import parse as urlparse

import redis
import redis.retry
from redis.asyncio import Redis, Sentinel, RedisCluster
from redis.asyncio.retry import Retry
from redis.backoff import FullJitterBackoff, NoBackoff

R = TypeVar('R', bound=redis.Redis)
CLIENT_NAME = socket.gethostname().rsplit('-', 2)[0]
DEFAULT_RETRY_ERRORS = (TimeoutError, socket.timeout, redis.TimeoutError, redis.ConnectionError)  # Needed for async version to add socket timeout

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
    headless: bool = False
    quorum: int = 0


def parse_url(url: str) -> ParsedRedisURL:
    url = urlparse.urlparse(url)

    def is_sentinel():
        return url.scheme == 'redis+sentinel' or url.scheme == 'sentinel' or url.scheme == 'sentinel+headless'

    def is_headless():
        return 'headless' in url.scheme

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
        'quorum': int,
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
        headless=is_headless(),
        quorum=options.get("quorum", 0),
    )


class HeadlessSentinel(Sentinel):
    def __init__(
        self,
        headless_host, port,
        min_other_sentinels=0,
        sentinel_kwargs=None,
        **connection_kwargs,
    ):
        self.headless_host = headless_host
        self.port = port
        super().__init__(
            sentinels=[(ip, port) for ip in self.sentinels_from_headless_host()],
            min_other_sentinels=min_other_sentinels,
            sentinel_kwargs=sentinel_kwargs,
            **connection_kwargs,
        )

    def sentinels_from_headless_host(self) -> list[str]:
        host, alias_list, ip_list = socket.gethostbyname_ex(self.headless_host)
        return ip_list

    def refresh_sentinels(self):
        self.sentinels = [
            Redis(host=ip, port=self.port, **self.sentinel_kwargs)
            for ip, port in self.sentinels_from_headless_host()
        ]

    # async def execute_command(self, *args, **kwargs):
    #     """
    #     Execute Sentinel command in sentinel nodes.
    #     once - If set to True, then execute the resulting command on a single
    #            node at random, rather than across the entire sentinel cluster.
    #     """
    #     try:
    #         return await super().execute_command(*args, **kwargs)
    #     except DEFAULT_RETRY_ERRORS:
    #         if not kwargs.get('sentinels_refreshed', False):
    #             self.refresh_sentinels()
    #             kwargs['sentinels_refreshed'] = True
    #             return await self.execute_command(*args, **kwargs)
    #         raise

    async def discover_master(self, service_name, has_refreshed: bool = False):
        try:
            return await super().discover_master(service_name)
        except DEFAULT_RETRY_ERRORS:
            if not has_refreshed:
                self.refresh_sentinels()
                return await self.discover_master(service_name, True)
            raise

    async def discover_slaves(self, service_name, has_refreshed: bool = False):
        try:
            return await super().discover_slaves(service_name)
        except DEFAULT_RETRY_ERRORS:
            if not has_refreshed:
                self.refresh_sentinels()
                return await self.discover_slaves(service_name, True)
            raise


class HeadlessSentinelSync(redis.Sentinel):
    def __init__(
        self,
        headless_host, port,
        min_other_sentinels=0,
        sentinel_kwargs=None,
        **connection_kwargs,
    ):
        self.headless_host = headless_host
        self.port = port
        super().__init__(
            sentinels=[(ip, port) for ip in self.sentinels_from_headless_host()],
            min_other_sentinels=min_other_sentinels,
            sentinel_kwargs=sentinel_kwargs,
            **connection_kwargs,
        )

    def sentinels_from_headless_host(self) -> list[str]:
        host, alias_list, ip_list = socket.gethostbyname_ex(self.headless_host)
        return ip_list

    def refresh_sentinels(self):
        self.sentinels = [
            redis.Redis(host=ip, port=self.port, **self.sentinel_kwargs)
            for ip, port in self.sentinels_from_headless_host()
        ]
    #
    # async def execute_command(self, *args, **kwargs):
    #     """
    #     Execute Sentinel command in sentinel nodes.
    #     once - If set to True, then execute the resulting command on a single
    #            node at random, rather than across the entire sentinel cluster.
    #     """
    #     try:
    #         return await super().execute_command(*args, **kwargs)
    #     except DEFAULT_RETRY_ERRORS:
    #         if not kwargs.get('sentinels_refreshed', False):
    #             self.refresh_sentinels()
    #             kwargs['sentinels_refreshed'] = True
    #             return await self.execute_command(*args, **kwargs)
    #         raise

    def discover_master(self, service_name, has_refreshed: bool = False):
        try:
            return super().discover_master(service_name)
        except DEFAULT_RETRY_ERRORS:
            if not has_refreshed:
                self.refresh_sentinels()
                return self.discover_master(service_name, True)
            raise

    def discover_slaves(self, service_name, has_refreshed: bool = False):
        try:
            return super().discover_slaves(service_name)
        except DEFAULT_RETRY_ERRORS:
            if not has_refreshed:
                self.refresh_sentinels()
                return self.discover_slaves(service_name, True)
            raise


def connect(redis_url: str,  read_only: bool = False, socket_timeout: float = None,
            redis_class: Type[R] = redis.Redis, decode_responses: bool = False) -> [R, redis.RedisCluster]:
    """Returns a synchronous redis-py connection

    Args:
        redis_url (str): The Redis connection url
        read_only (bool): If true, the connection will be made to the master node, else to a slave node
        socket_timeout (float): Socket timeout threshold
        redis_class (Type[redis.Redis]):
        decode_responses (bool): Redis connection kwarg for whether or not to decode_responses

    Returns:
        R: Redis connection
    """
    rinfo = parse_url(redis_url)
    if rinfo.cluster:
        host, port = rinfo.hosts[0]
        return redis.RedisCluster(
            host=host,
            port=port,
            # client params
            # db=rinfo.database,
            password=rinfo.password,
            decode_responses=decode_responses,
            socket_timeout=socket_timeout or rinfo.socket_timeout,
            health_check_interval=30,
            retry=redis.retry.Retry(FullJitterBackoff(), 1),
            client_name=CLIENT_NAME,
        )

    elif rinfo.sentinel:  # We're connecting to a sentinel cluster.
        # establish how many retries would be required to check all IPs of a headless service
        if rinfo.headless:
            # min_retries = max([
            #     len(socket.getaddrinfo(host, port, type=socket.SOCK_STREAM))
            #     for host, port in rinfo.hosts
            # ])
            sentinel_connection = HeadlessSentinelSync(
                rinfo.hosts[0][0],
                rinfo.hosts[0][1],
                min_other_sentinels=rinfo.quorum or 0,
                sentinel_kwargs={
                    'retry': redis.retry.Retry(NoBackoff(), 3),
                    'socket_timeout': socket_timeout or rinfo.socket_timeout,
                    'health_check_interval': 30,
                    'client_name': CLIENT_NAME,
                },
                ## connection kwargs that will be applied to masters/slaves if not overridden
                db=rinfo.database,
                password=rinfo.password,
                health_check_interval=30,
                retry=redis.retry.Retry(FullJitterBackoff(), 1),
                client_name=CLIENT_NAME,
            )
        else:
            sentinel_connection = redis.Sentinel(
                rinfo.hosts,
                min_other_sentinels=rinfo.quorum or 0,
                sentinel_kwargs={
                    'retry': redis.retry.Retry(NoBackoff(), 3),
                    'socket_timeout': socket_timeout or rinfo.socket_timeout,
                    'health_check_interval': 30,
                    'client_name': CLIENT_NAME,
                },
                ## connection kwargs that will be applied to masters/slaves if not overridden
                db=rinfo.database,
                password=rinfo.password,
                health_check_interval=30,
                retry=redis.retry.Retry(FullJitterBackoff(), 1),
                client_name=CLIENT_NAME,
            )
        if read_only:
            return sentinel_connection.slave_for(
                rinfo.service_name,
                redis_class=redis_class,
                socket_timeout=socket_timeout or rinfo.socket_timeout,
                decode_responses=decode_responses,
                client_name=CLIENT_NAME,
            )
        else:
            return sentinel_connection.master_for(
                rinfo.service_name,
                redis_class=redis_class,
                socket_timeout=socket_timeout or rinfo.socket_timeout,
                decode_responses=decode_responses,
                client_name=CLIENT_NAME,
            )
    else:  # Single redis instance
        host, port = rinfo.hosts[0]
        return redis_class(
            host=host,
            port=port,
            db=rinfo.database,
            password=rinfo.password,
            decode_responses=decode_responses,
            socket_timeout=socket_timeout or rinfo.socket_timeout,
            health_check_interval=30,
            retry=redis.retry.Retry(FullJitterBackoff(), 1),
            client_name=CLIENT_NAME,
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
    if rinfo.cluster:
        host, port = rinfo.hosts[0]
        cluster_retry = Retry(FullJitterBackoff(), 1)
        cluster_retry.update_supported_errors(list(DEFAULT_RETRY_ERRORS))   # can't pass retry_on_timeout to async cluster
        return RedisCluster(
            host=host,
            port=port,
            # client params
            db=rinfo.database,
            password=rinfo.password,
            decode_responses=decode_responses,
            socket_timeout=socket_timeout or rinfo.socket_timeout,
            health_check_interval=30,
            retry=cluster_retry,
            client_name=CLIENT_NAME,
        )
    elif rinfo.sentinel:
        # We're connecting to a sentinel cluster.
        if rinfo.headless:
            # min_retries = max([
            #     len(socket.getaddrinfo(host, port, type=socket.SOCK_STREAM))
            #     for host, port in rinfo.hosts
            # ])
            sentinel_connection = HeadlessSentinel(
                rinfo.hosts[0][0],
                rinfo.hosts[0][1],
                min_other_sentinels=rinfo.quorum or 0,
                sentinel_kwargs={
                    'retry': redis.retry.Retry(NoBackoff(), 3),
                    'socket_timeout': socket_timeout or rinfo.socket_timeout,
                    'health_check_interval': 30,
                    'client_name': CLIENT_NAME,
                },
                ## connection kwargs that will be applied to masters/slaves if not overridden
                db=rinfo.database,
                password=rinfo.password,
                health_check_interval=30,
                retry=redis.retry.Retry(FullJitterBackoff(), 1),
                client_name=CLIENT_NAME,
            )
        else:
            sentinel_connection = Sentinel(
                rinfo.hosts,
                sentinel_kwargs={
                    'retry_on_timeout': True,  # required for retry on socket timeout for the async class
                    'retry': Retry(NoBackoff(), 3),
                    'socket_timeout': socket_timeout or rinfo.socket_timeout,
                    'health_check_interval': 30,
                    'client_name': CLIENT_NAME,
                },
                ## connection kwargs that will be applied to masters/slaves if not overridden
                db=rinfo.database,
                password=rinfo.password,
                health_check_interval=30,
                retry_on_timeout=True,  # required for retry on socket timeout for the async class
                retry=Retry(FullJitterBackoff(), 1),
                client_name=CLIENT_NAME,
            )
        if read_only:
            return sentinel_connection.slave_for(
                rinfo.service_name,
                redis_class=redis_class,
                socket_timeout=socket_timeout or rinfo.socket_timeout,
                decode_responses=decode_responses,
                client_name=CLIENT_NAME,
            )
        else:
            return sentinel_connection.master_for(
                rinfo.service_name,
                redis_class=redis_class,
                socket_timeout=socket_timeout or rinfo.socket_timeout,
                decode_responses=decode_responses,
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
            socket_timeout=socket_timeout or rinfo.socket_timeout,
            retry_on_timeout=True,  # required for retry on socket timeout for the async class
            retry=Retry(FullJitterBackoff(), 1),
            client_name=CLIENT_NAME,
        )
