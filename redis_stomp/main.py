import argparse
import anyio
import uvicorn
from uvicorn.config import LOGGING_CONFIG
import logging
from starlette.applications import Starlette
from starlette.routing import WebSocketRoute

from starlette.endpoints import WebSocketEndpoint, WebSocket
from starlette import status

import coilmq.util.frames
from coilmq.asyncio.engine import StompEngine
from coilmq.asyncio.server import StompConnection
from coilmq.asyncio.protocol import STOMP11
from coilmq.util.frames import FrameBuffer
from coilmq.exception import ClientDisconnected
from redis_stomp.pubsub.topic import RedisTopicManager

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
        app = self.scope["app"]
        try:
            self.buffer = FrameBuffer()
            self.stomp_engine = StompEngine(
                connection=self,
                authenticator=None,  # ToDo: perhaps basic auth
                queue_manager=None,
                topic_manager=RedisTopicManager(self, app.state.redis_url),
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
        LOGGER.debug("SEND: %r" % packed)

        await self.websocket.send_text(packed)

    async def send_heartbeat(self):
        """ Sends an EOL to connected socket client."""
        heartbeat = '\n'
        LOGGER.debug("SEND: %r" % heartbeat)
        await self.websocket.send_text(heartbeat)


routes = [
    WebSocketRoute("/ws", StompEndpoint, name='stomp_ws'),
]


app = Starlette(
    routes=routes,
)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--port', type=int, nargs='?', const=8000, default=8000, help='Port for web server.')
    parser.add_argument('-r', '--redis', nargs='?', const='redis://localhost:6379', default='redis://localhost:6379', help='URL to connect to Redis')
    args = parser.parse_args()
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
