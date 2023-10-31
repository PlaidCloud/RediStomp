import asyncio
import glob
import logging
import uuid

from coilmq.asyncio.topic import TopicManager
from coilmq.asyncio.server import StompConnection
from coilmq.util.frames import Frame, MESSAGE

from redis_stomp.redis_connector import aio_connect, connect


LOGGER = logging.getLogger(__name__)
TOPIC_PREFIX = '/topic/'

class RedisTopicManager(TopicManager):
    """
    Class that manages distribution of messages to topic subscribers.

    This class uses C{threading.RLock} to guard the public methods.  This is probably
    a bit excessive, given 1) the atomic nature of basic C{dict} read/write operations
    and  2) the fact that most of the internal data structures are keying off of the
    STOMP connection, which is going to be thread-isolated.  That said, this seems like
    the technically correct approach and should increase the chance of this code being
    portable to non-GIL systems.

    @ivar _subscriptions: A dict of registered topics, keyed by destination.
    @type _subscriptions: C{dict} of C{str} to C{set} of L{coilmq.server.StompConnection}
    """

    def __init__(self, connection: StompConnection, redis_url: str):
        super().__init__()
        self._closing = False
        self._closed = False
        self._redis = connect(redis_url, decode_responses=True).pubsub(ignore_subscribe_messages=True)

    async def disconnect(self, connection: StompConnection):
        await super().disconnect(connection)
        await self.close()

    async def close(self):
        self._closing = True
        while not self._closed:
            await asyncio.sleep(0.1)
        if self._redis:
            await asyncio.to_thread(self._redis.close)

    def get_redis_destination(self, destination):
        dest = destination.replace('#', '*')
        if dest.startswith(TOPIC_PREFIX):
            return dest[7:]
        return dest

    async def subscribe(self, connection: StompConnection, destination: str, id: str = None):
        """
        Subscribes a connection to the specified topic destination.

        @param connection: The client connection to subscribe.
        @type connection: L{coilmq.asyncio.server.StompConnection}

        @param destination: The topic destination (e.g. '/topic/foo')
        @type destination: C{str}

        @param id: subscription identifier (optional)
        @type id: C{str}
        """
        redis_destination = self.get_redis_destination(destination)
        await super().subscribe(connection, redis_destination, id)
        if glob.has_magic(redis_destination):
            LOGGER.debug(f'PSUBSCRIBE: {redis_destination}')
            await asyncio.to_thread(self._redis.psubscribe, redis_destination)
        else:
            LOGGER.debug(f'SUBSCRIBE: {redis_destination}')
            await asyncio.to_thread(self._redis.subscribe, redis_destination)


    async def unsubscribe(self, connection: StompConnection, destination: str = None, id: str = None):
        """
        Unsubscribes a connection from the specified topic destination.

        @param connection: The client connection to unsubscribe.
        @type connection: L{coilmq.asyncio.server.StompConnection}

        @param destination: The topic destination (e.g. '/topic/foo') (optional)
        @type destination: C{str}

        @param id: subscription identifier (optional)
        @type id: C{str}
        """
        if id and not destination:
            redis_destination = self._subscriptions.destination_for_id(id)
        else:
            redis_destination = self.get_redis_destination(destination)
        if not redis_destination:
            return
        await super().unsubscribe(connection, redis_destination, id)
        if glob.has_magic(redis_destination):
            LOGGER.debug(f'PUNSUBSCRIBE: {redis_destination}')
            await asyncio.to_thread(self._redis.punsubscribe, redis_destination)
        else:
            LOGGER.debug(f'UNSUBSCRIBE: {redis_destination}')
            await asyncio.to_thread(self._redis.unsubscribe, redis_destination)

    async def next_message(self):
        # ToDo: This could be done by registering a callback on subscribe which might be more favorable, I dunno
        # await redis_pubsub.subscribe(**{ACTIVITY_CHANNEL: on_message})
        while not self._closing:
            if not self._redis.subscribed:
                await asyncio.sleep(0.5)
                continue
            message = await asyncio.to_thread(self._redis.get_message, ignore_subscribe_messages=True, timeout=0.5)
            # get_message with timeout=None can return None
            if message:
                # print(repr(message))
                # Lookup the subscriptions by pattern/channel
                dest = message['channel'] if message['type'] == 'message' else message['pattern']

                bad_subscribers = set()
                for subscriber in await self._subscriptions.subscribers(dest):
                    frame = Frame(cmd=MESSAGE)
                    frame.headers.setdefault('message-id', str(uuid.uuid4()))
                    frame.headers['destination'] = TOPIC_PREFIX + message['channel']
                    frame.headers["subscription"] = subscriber.id
                    frame.body = message['data']
                    try:
                        await subscriber.connection.send_frame(frame)
                    except:
                        self.log.exception(
                            "Error delivering message to subscriber %s; client will be disconnected." % subscriber)
                        # We queue for deletion so we are not modifying the topics dict
                        # while iterating over it.
                        bad_subscribers.add(subscriber)

                for s in bad_subscribers:
                    await self.unsubscribe(s.connection, dest, id=s.id)

        self._closed = True