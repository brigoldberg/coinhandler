#!/usr/bin/env python3
import asyncio
import json
import logging
import signal
import sys
import websockets
from collections import deque
from prometheus_client import Counter
from prometheus_client import start_http_server

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from contextlib import asynccontextmanager
from utils import cli_args, parse_config

def signal_handler(sig, frame):
    logger.info("Exiting feed handler")
    sys.exit(0)
signal.signal(signal.SIGINT, signal_handler)
logger = logging.getLogger()
logging.basicConfig(format='%(levelname)s - %(message)s',level=logging.INFO)

# Prometheus Metrics
tickers_in = Counter('price_ticks_rcvd', 'Price ticks received from exch')
msg_published = Counter('msg_published', 'Messages published to clients')


class FeedHandler:

    def __init__(self, product_id, websocket_url, queue_len):
        self.product_id = product_id
        self.websocket_url = websocket_url
        self.queue = deque(maxlen=queue_len)

        self.connected_clients = set()
        self.app = FastAPI(lifespan=self.lifespan)
        self.setup_routes()

    def setup_routes(self):
        """Define the API and WebSocket routes."""
        @self.app.websocket("/subscribe")
        async def websocket_endpoint(websocket: WebSocket):
            await websocket.accept()
            self.connected_clients.add(websocket)
            logger.info(f"Client connected: {websocket.client}")
            try:
                while True:
                    # Keep the connection alive
                    await websocket.receive_text()
            except WebSocketDisconnect:
                logger.info(f"Client disconnected: {websocket.client}")
                self.connected_clients.remove(websocket)

        @self.app.get("/latest")
        def get_latest_data():
            """Retrieve the latest price data."""
            if self.queue:
                print("Returning latest price data.")
                return list(self.queue)[-1]
            logger.debug("No price data available to client.")
            return {"error": "No data available"}

        @self.app.get("/health")
        def health_check():
            logger.debug("Health check performed.")
            """Health check endpoint."""
            if len(self.queue) > 1:
                return {"status": "feedhandler_healthy"}
            else:
                raise HTTPException(status_code=503, detail="Service not ready")

    @asynccontextmanager
    async def lifespan(self, app: FastAPI):
        """Lifespan context for startup and shutdown events."""
        self.task = asyncio.create_task(self.consume())
        logger.info("WebSocket feed handler started.")
        yield
        self.task.cancel()
        try:
            await self.task
        except asyncio.CancelledError:
            logger.error("WebSocket feed handler stopped.")


    async def subscribe(self, websocket):
        """Send a subscription message to exchange"""
        subscription_msg = {
            "type": "subscribe",
            "channels": [
                {
                    "name": "ticker",
                    "product_ids": [self.product_id]
                }
            ]
        }
        await websocket.send(json.dumps(subscription_msg))
        logger.info(f'Subscribed to {self.product_id} feed')

    async def consume(self):
        """Consume messages from websocket"""
        async with websockets.connect(self.websocket_url) as websocket:
            await self.subscribe(websocket)

            try:
                async for message in websocket:
                    tickers_in.inc()
                    message = json.loads(message)
                    if message['type'] == 'ticker':
                        await self.process_message(message)
            except websockets.ConnectionClosed as e:
                logger.info(f'Connection closed: {e}')
            except Exception as e:
                logger.error(f'Error occured: {e}')

    async def process_message(self, message):
        """Process received message"""
        price = message.get('price')
        time = message.get('time')
        last_size = message.get('last_size')
        if price and time:
            message = {
                "time": time,
                "price": price,
                "size": last_size
            }
            self.queue.append(message)
            await self.broadcast_to_clients(message)

    async def broadcast_to_clients(self, message):
        """Publish message to all connected clients"""
        if self.connected_clients:
            msg_published.inc()
            message = json.dumps(message)
            await asyncio.gather(*[client.send_text(message) for client in self.connected_clients])

if __name__ == '__main__':

    args = cli_args()
    config = parse_config(args.cfg_fn, 'feedhandler')
    
    import uvicorn
    start_http_server(config['prom_exporter_port'])

    solana_fh = FeedHandler(config['ticker'], config['feed_uri'], config['queue_size'])
    uvicorn.run(solana_fh.app, host='0.0.0.0', port=config['subscription_port'])
