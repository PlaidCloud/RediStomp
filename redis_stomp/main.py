import argparse
import os
import logging

import anyio
import uvicorn
from uvicorn.config import LOGGING_CONFIG
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import Response
from starlette.routing import Route, WebSocketRoute
from starlette.endpoints import WebSocketEndpoint, WebSocket
from starlette import status
import coilmq.util.frames
from coilmq.asyncio.engine import StompEngine
from coilmq.asyncio.server import StompConnection
from coilmq.asyncio.protocol import STOMP11
from coilmq.util.frames import FrameBuffer
from coilmq.exception import ClientDisconnected
from redis.asyncio import Redis

from redis_stomp.pubsub.topic import RedisTopicManager
from redis_stomp.redis_connector import aio_connect

logging.basicConfig(
    level = logging.DEBUG,
    format = "[%(asctime)s] %(levelname)s [%(name)s.%(funcName)s:%(lineno)d] %(message)s",
)

LOGGER = logging.getLogger(__name__)

class StompEndpoint(WebSocketEndpoint, StompConnection):

    async def dispatch(self) -> None:
        self.websocket = websocket = WebSocket(self.scope, receive=self.receive, send=self.send)
        await websocket.accept(headers=[(b'sec-websocket-protocol', b'v11.stomp')])

        close_code = status.WS_1000_NORMAL_CLOSURE

        try:
            self.buffer = FrameBuffer()
            self.topic_manager = RedisTopicManager(self, self.scope["app"].state.redis_url)
            self.stomp_engine = StompEngine(
                connection=self,
                authenticator=None,  # ToDo: perhaps basic auth
                queue_manager=None,
                topic_manager=self.topic_manager,
                protocol=STOMP11,  # Note, this matches the protocol in the websocket accept above, but it should be negotiated
            )

            async with anyio.create_task_group() as task_group:
                # run until first is complete
                async def run_ws_receiver() -> None:
                    await self.ws_receiver(websocket=websocket)
                    task_group.cancel_scope.cancel()

                task_group.start_soon(run_ws_receiver)
                await self.stomp_engine.topic_manager.next_message()


        except ClientDisconnected:
            close_code = status.WS_1000_NORMAL_CLOSURE  # Likely really a failing stomp heartbeat
        except Exception as exc:
            close_code = status.WS_1011_INTERNAL_ERROR
            raise exc
        finally:
            await self.stomp_engine.unbind()
            await self.topic_manager.close()
            await self.on_disconnect(websocket, close_code)

    async def ws_receiver(self, websocket: WebSocket):
        async for data in websocket.iter_text():
            self.buffer.append(data.encode())

            if not self.buffer.buffer_empty():
                await self.stomp_engine.protocol.process_heartbeat()

            for frame in self.buffer:
                LOGGER.debug("Processing frame: %s" % frame)
                await self.stomp_engine.process_frame(frame)
                if not self.stomp_engine.connected:
                    raise ClientDisconnected()


    async def send_frame(self, frame: coilmq.util.frames.Frame):
        """ Sends a frame to connected socket client.
        """
        packed = frame.pack()
        packed_str = packed.decode()
        LOGGER.debug("SEND: %r" % packed_str)

        await self.websocket.send_text(packed_str)

    async def send_heartbeat(self):
        """ Sends an EOL to connected socket client."""
        heartbeat = '\n'
        LOGGER.debug("SEND: %r" % heartbeat)
        await self.websocket.send_text(heartbeat)

async def alive_probe(request: Request):
    async with aio_connect(
        request.app.state.redis_url,
        decode_responses=True,
    ).pubsub(
        ignore_subscribe_messages=True,
    ) as redis_con:
        await redis_con.ping()
    return Response()


routes = [
    WebSocketRoute("/ws", StompEndpoint, name='stomp_ws'),
    Route('/alive', alive_probe, methods=['GET']),
]


app = Starlette(
    routes=routes,
)

def main():
    parser = argparse.ArgumentParser()
    default_port = os.environ.get('PORT', 8000)
    parser.add_argument('-p', '--port', type=int, nargs='?', const=default_port, default=default_port, help='Port for web server.')
    default_redis_url = os.environ.get('REDIS_URL', 'redis://localhost:6379')
    parser.add_argument('-r', '--redis', nargs='?', const=default_redis_url, default=default_redis_url, help='URL to connect to Redis')
    args = parser.parse_args()
    LOGGER.info(f'Connecting to Redis at {args.redis}, serving on port {args.port}')
    try:
        LOGGING_CONFIG["formatters"]["default"]["fmt"] = "%(asctime)s [%(name)s] %(levelprefix)s %(message)s"
        app.state.redis_url = args.redis
        uvicorn.run(app, host="0.0.0.0", port=args.port, access_log=False, proxy_headers=True, forwarded_allow_ips='*', log_level='debug')
    except KeyboardInterrupt:
        pass
    except Exception as e:
        LOGGER.exception('Server terminated due to error: %s' % e)
        raise

    LOGGER.info('Gracefully exiting')

if __name__ == "__main__":
    main()
