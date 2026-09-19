# 2 way OCPP proxy (can also be used as a 1-way simple proxy)

import asyncio
import logging
import time
from typing import Tuple
import json

import websockets
import websockets.asyncio
import websockets.asyncio.server

from enum import IntEnum
import ssl
import argparse
import configparser

__version__ = "0.1.0"

config = configparser.ConfigParser()

logging.basicConfig(
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("proxy")

class OCPP2WProxy: # Forward declaration
    pass

class OCPPMessageType(IntEnum):
    Call = 2
    CallResult = 3
    CallError = 4

# main class
class OCPP2WProxy:
    # Static dict of OCPP2WProxy instances. key is charger_id
    proxy_list: dict[str, OCPP2WProxy] = {}

    # Utility functions
    @staticmethod
    def decode_ocpp_message(message: str) -> Tuple[OCPPMessageType, str]:
        """Decode an OCPP message from a string"""
        j = json.loads(message)
        return [j[0], j[1]]

    def __init__(self, websocket: websockets.asyncio.server.ServerConnection, charger_id: str):
        # Store the websocket for later     
        logger.debug(websocket.request)
        self.ws = websocket
        self.charger_id = charger_id

        # Chech that charger id looks reasonable
        if not charger_id.isalnum():
            logger.error(f"Charger ID '{charger_id}' is not alphanumeric")
            raise Exception("Charger ID is not alphanumeric")

        # Track each upstream call so the charger's result can be sent back to
        # the server that originated it.
        self.server_call_ids: dict[str, int] = {}
        self.server_connections = []

        # Insert new OCPP2WProxy instance in the (static) dict of instances.
        self.proxy_list[charger_id] = self

    async def close(self):
        """Close the charger connection and all upstream server connections."""
        try:
            await self.ws.close()
            for connection in self.server_connections:
                await connection.close()
        except Exception as e:
            pass # Ignore exceptions

    @staticmethod
    async def check_delete_old(charger_id: str):
        """Check if there are any old instances of this charger in the proxy list"""
        if charger_id in OCPP2WProxy.proxy_list:
            logger.info(f"Charger ID {charger_id} already exists. Closing and deleting")
            proxy: OCPP2WProxy = OCPP2WProxy.proxy_list[charger_id]
            await proxy.close()
            del OCPP2WProxy.proxy_list[charger_id]

    async def run(self):
        """Main loop for this proxy. This is where all the magic happens."""

        # Create connections to all configured CSMSes.
        # Forward any available Authorization and User-Agent headers
        headers = {}
        if "Authorization" in self.ws.request.headers:
            headers["Authorization"] = self.ws.request.headers["Authorization"]
            logger.debug(f'Authorization header set to {headers["Authorization"]}')
        user_agent = self.ws.request.headers.get("User-Agent", None) 
        subprotocols = self.ws.request.headers.get("Sec-WebSocket-Protocol", ["ocpp1.6"])
        if config.has_option("ext-server", "servers"):
            server_urls = [
                url.strip().rstrip("/")
                for url in config.get("ext-server", "servers").split(",")
                if url.strip()
            ]
        else:
            server_urls = [config.get("ext-server", "server").strip().rstrip("/")]
            if config.has_option("ext-server", "secondary_server"):
                server_urls.append(
                    config.get("ext-server", "secondary_server").strip().rstrip("/")
                )

        if not server_urls:
            raise ValueError("At least one external server must be configured")

        try:
            for server_url in server_urls:
                self.server_connections.append(
                    await websockets.connect(
                        uri=f"{server_url}/{self.charger_id}",
                        user_agent_header=user_agent,
                        additional_headers=headers,
                        subprotocols=[subprotocols],
                    )
                )
            for index, server_url in enumerate(server_urls):
                role = "primary" if index == 0 else f"secondary {index}"
                logger.info(
                    f"{self.charger_id} Connected to {role} server @ "
                    f"{server_url}/{self.charger_id}"
                )

            # Create tasks to handle the charger and each configured server.
            self._last_charger_update = time.time()
            self.tasks = [asyncio.create_task(self.receive_charger_messages())]
            self.tasks.extend(
                asyncio.create_task(self.receive_server_messages(index))
                for index in range(len(self.server_connections))
            )
            #self.tasks.append(asyncio.create_task(self.watchdog()))

            # Wait for tasks to complete
            done, pending = await asyncio.wait(self.tasks, return_when=asyncio.FIRST_COMPLETED)
            logger.debug(f"{self.charger_id} Task(s) completed: {done}, {pending}")

            for task in done:
                e = task.exception()
                if e:
                    logger.warning(f"{self.charger_id} (Not serious - likely connection loss) Task {task} raised exception {e} related to charger ")

            # Cancel any remaining tasks
            for task in pending:
                task.cancel()

        except websockets.exceptions.InvalidURI:
            logger.error(f"{self.charger_id} Invalid URI")
        except websockets.exceptions.ConnectionClosedError as e:
            logger.error(f"{self.charger_id} Connection closed unexpectedly: {e}")
        except websockets.exceptions.InvalidHandshake:
            logger.error(f"{self.charger_id} Handshake with the external server failed")
        except Exception as e:
            logger.error(f"{self.charger_id} Unexpected error: {e}")
        finally:
            # Always close stuff. close is well tempered, so can close even if not stablished
            await self.close()

    async def receive_charger_messages(self):
        try:
            while True:
                # Wait for a message from the charger
                message = await self.ws.recv()
                logger.info(f"{self.charger_id} ^ : {message}")

                [message_type, message_id] = OCPP2WProxy.decode_ocpp_message(message)

                # Calls from the charger are broadcast to every upstream server.
                if message_type == OCPPMessageType.Call:
                    await asyncio.gather(
                        *(connection.send(message) for connection in self.server_connections)
                    )
                elif message_type == OCPPMessageType.CallResult or message_type == OCPPMessageType.CallError:
                    server_index = self.server_call_ids.pop(message_id, None)
                    if server_index is None:
                        logger.error(f"{self.charger_id} ^: Received CallResult/CallError against unknown message id {message_id}")
                    else:
                        logger.info(f"{self.charger_id} ^ : Result/Error forwarded to server {server_index}")
                        await self.server_connections[server_index].send(message)
                else:
                    logger.error(f"{self.charger_id} ^: Unknown message type {message_type}")
        except Exception as e:
            logger.error(f"{self.charger_id} Error in receive_charger_messages: {e}")

    async def receive_server_messages(self, server_index: int):
        connection = self.server_connections[server_index]
        role = "primary" if server_index == 0 else f"secondary {server_index}"
        try:
            while True:
                message = await connection.recv()
                logger.info(f"{self.charger_id} v ({role}) : {message}")

                [message_type, message_id] = OCPP2WProxy.decode_ocpp_message(message)
                if message_type == OCPPMessageType.Call:
                    self.server_call_ids[message_id] = server_index
                    await self.ws.send(message)
                elif server_index == 0:
                    await self.ws.send(message)
        except Exception as e:
            logger.error(f"{self.charger_id} Error in receive_{role}_messages: {e}")

    async def receive_primary_messages(self):
        await self.receive_server_messages(0)

    async def receive_secondary_messages(self):
        await self.receive_server_messages(1)

    async def watchdog(self):
        """Watch time vs. timestamp updated by receiving messages from charger."""
        while True:
            # And ... sleep
            await asyncio.sleep(config.getint("host", "watchdog_interval", 30))

            elapsed = time.time() - self._last_charger_update
            if elapsed > config.getint("host", "watchdog_stale", 300):
                logger.error(f"{self.charger_id} Watch dog no for {elapsed} seconds. Closing connections")
                return

# Connection handler (charger connects)
async def on_connect(websocket: websockets.asyncio.server.ServerConnection):
    logger.debug('Connection request', websocket.request)
    # Determine charger_id (final part of path)
    path = websocket.request.path
    charger_id = path.strip("/")
    logger.info(f'{charger_id} connection request')

    try:
        # Delete any existing charger setup
        if charger_id in OCPP2WProxy.proxy_list:
            await OCPP2WProxy.proxy_list[charger_id].close()
            del OCPP2WProxy.proxy_list[charger_id]

        # Setup
        proxy = OCPP2WProxy(websocket=websocket, charger_id=charger_id)

        # Connect and run proxy operations
        await proxy.run()

    except Exception as e:
        logger.error(f'{charger_id} Error creating OCPP2WProxy: {e}')
    finally:
        logger.info("f{charger_id} closed/done")


# Main. Decode arguments, setup handler
async def main():
    parser = argparse.ArgumentParser(
        description='ocpp-2w-proxy: A two way OCPP proxy')
    parser.add_argument('--version', action='version',
                        version=f'%(prog)s {__version__}')
    parser.add_argument(
        "--config",
        type=str,
        default="ocpp-2w-proxy.ini",
        help="Configuration file (INI format). Default ocpp-2w-proxy.ini",
    )
    args = parser.parse_args()

    # Read config. config object is then available (via config import) to all.
    logger.warning(f"Reading config from {args.config}")
    config.read(args.config)

    # Adjust log levels
    for logger_name in config["logging"]:
        logger.warning(f'Setting log level for {logger_name} to {config.get("logging", logger_name)}')
        logging.getLogger(logger_name).setLevel(level=config.get("logging", logger_name))

    # Get host config
    host = config.get("host", "addr")
    port = config.get("host", "port")
    cert_chain = config.get("host", "cert_chain", fallback=None)
    cert_key = config.get("host", "cert_key", fallback=None)
    logger.debug(f"host: {host}, port: {port}, cert_chain: {cert_chain}, cert_key: {cert_key}")

    # Start server, either ws:// or wss://
    if cert_chain and cert_key:
        ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ssl_context.load_cert_chain(certfile=cert_chain, keyfile=cert_key)
        server = await websockets.serve(
            on_connect,
            host,
            port,
            subprotocols=["ocpp1.6", "ocpp2.0.1"],
            ssl=ssl_context,
            ping_timeout=config.getint("host", "ping_timeout"),
        )
    else:
        server = await websockets.serve(
            on_connect,
            host,
            port,
            subprotocols=["ocpp1.6", "ocpp2.0.1"],
            ping_timeout=config.getint("host", "ping_timeout"),
        )

    logger.info("Proxy ready. Waiting for new connections...")
    await server.wait_closed()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        exit(0)
