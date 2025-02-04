#!/usr/bin/env python3
import asyncio
import json
import logging
import signal
import sys
import websockets
from collections import deque
from datetime import datetime, timedelta
from fastapi import FastAPI
from prometheus_client import Counter, Gauge
from prometheus_client import start_http_server
from utils import cli_args, parse_config

from contextlib import asynccontextmanager

logger = logging.getLogger(__name__)
logging.basicConfig(format='%(levelname)s - %(message)s',level=logging.INFO)

# Prometheus Metrics
msg_recv = Counter('msg_recv', 'Messages recieved')
msg_unpacked = Counter('msg_unpacked', 'Messages unpacked')
bars_created = Counter('bars_created', 'Sum of all bars created')
bar_queue_len = Gauge('bar_queue_len', 'Total number of bars in queue')

def unpack_tick_msg(message):
    """
    Unpacks a WebSocket message into fields.
    Returns:
        tick_time:  datetime
        price:      float
        volume:     float
    """
    json_msg = json.loads(message)
    tick_time = datetime.fromisoformat(json_msg["time"])
    price = float(json_msg["price"])
    volume = float(json_msg["size"])
    msg_unpacked.inc()
    return tick_time, price, volume


class BarAggregator:
    """
    Aggregates ticks into 1-minute bars.
    """
    def __init__(self):
        self.current_bar = None
        self.bar_queue = deque(maxlen=1440)
        self.tick_queue = deque(maxlen=1440)
    
    def process_tick(self, tick_time, price, volume):
        """
        Processes a single tick and updates the 1-minute bar.
        """
        bar_start_time = tick_time.replace(second=0, microsecond=0)
        self.tick_queue.append({"time": tick_time, "price": price, "volume": volume})

        # If it's a new bar period
        if self.current_bar is None or self.current_bar["start_time"] != bar_start_time:
            # Save the completed bar
            if self.current_bar:
                # self.bars.append(self.current_bar)
                self.log_bar(self.current_bar)
                self.bar_queue.append(self.current_bar)
                bar_queue_len.set(len(self.bar_queue))
                bars_created.inc()

            # Start a new bar
            self.current_bar = {
                "start_time": bar_start_time,
                "open": price,
                "high": price,
                "low": price,
                "close": price,
                "volume": volume,
                "pv_sum": (price * volume),
                "vwap": (price * volume) / volume
            }
        else:
            # Update the existing bar
            self.current_bar["high"] = max(self.current_bar["high"], price)
            self.current_bar["low"] = min(self.current_bar["low"], price)
            self.current_bar["close"] = price
            self.current_bar["volume"] += volume
            self.current_bar["pv_sum"] += (price * volume)
            self.current_bar["vwap"] = ((price * volume) + self.current_bar["pv_sum"]) \
                                            / self.current_bar["volume"]

    def get_last_tick(self):
        if len(self.tick_queue) >= 1:
            return self.tick_queue[-1]
        else:
            return {}

    def get_last_bar(self):
        if len(self.bar_queue) >= 1:
            return self.bar_queue[-1]
        else:
            return {}

    def log_bar(self, bar):
        """
        Logs a completed 1-minute bar.
        """
        logger.info(f"Bar: {bar['start_time']}, Open: {bar['open']}, High: {bar['high']}, "
              f"Low: {bar['low']}, Close: {bar['close']}, Volume: {bar['volume']:0.2f}, VWAP: {bar['vwap']:0.2f}")


class Vwap:

    def __init__(self, uri):
        self.app = FastAPI(lifespan=self.lifespan)
        self.uri = uri
        self.setup_routes()
        self.bar_agg = BarAggregator()       

    async def consume(self):
        """Consume market data feed from internal feed handler"""
        async with websockets.connect(self.uri) as websocket:
            try:
                async for message in websocket:
                    msg_recv.inc()
                    tick_time, price, volume = unpack_tick_msg(message)
                    self.bar_agg.process_tick(tick_time, price, volume)
            except websockets.ConnectionClosed as e:
                logger.info(f"Connection closed: {e}")
            except Exception as e:
                logger.error(f"Error occured: {e}")

    def setup_routes(self):
        """Define API interfaces"""
        @self.app.get("/health")
        def health_check():
            return {"status": "vwap_engine_healthy"}

        @self.app.get("/lastbar")
        def get_last_bar():
            result = self.bar_agg.get_last_bar()
            return result

        @self.app.get("/lasttick")
        def get_last_tick():
            result = self.bar_agg.get_last_tick()
            return result

        @self.app.get("/tradesig")
        def trade_signal():
            last_tick = get_last_tick()
            last_bar = get_last_bar()
            if len(last_tick) >= 1 and len(last_bar) >= 1:
                result = self.calc_vwap_signal(last_tick, last_bar)
                return {"signal": result}
            else:
                return {"signal": "neutral"}

    @asynccontextmanager
    async def lifespan(self, app: FastAPI):
        """Lifespan context for startup and shutdown events."""
        self.task = asyncio.create_task(self.consume())
        logger.info("VWAP feed consumer started.")
        yield
        self.task.cancel()
        try:
            await self.task
        except asyncio.CancelledError:
            logger.error("VWAP feed consumer stopped.")    

    def calc_vwap_signal(self, tick, vwap_bar, sig_threshold=0.05):
        """Input current price and return buy/sell signal"""
        price_vwap_delta = tick['price'] - vwap_bar['vwap'] /  vwap_bar['vwap']

        if abs(price_vwap_delta) > sig_threshold:
            if price_vwap_delta > 0:
                return "sell"
            else:
                return "buy"


if __name__ == '__main__':

    args = cli_args()
    config = parse_config(args.cfg_fn, 'vwap')
    start_http_server(config['prom_exporter_port'])

    import uvicorn
    vwap = Vwap(config['feed_uri'])
    uvicorn.run(vwap.app, host="0.0.0.0", port=config['subscription_port'])