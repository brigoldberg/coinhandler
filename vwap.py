#!/usr/bin/env python3

import asyncio
import json
import logging
import signal
import sys
import websockets
from collections import deque
from datetime import datetime, timedelta
from prometheus_client import Counter, Gauge
from prometheus_client import start_http_server

def signal_handler(sig, frame):
    logger.info("Exiting VWAP client")
    sys.exit(0)

URI = "ws://localhost:8001/subscribe"
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
    
    def process_tick(self, tick_time, price, volume):
        """
        Processes a single tick and updates the 1-minute bar.
        """
        bar_start_time = tick_time.replace(second=0, microsecond=0)

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

    def log_bar(self, bar):
        """
        Logs a completed 1-minute bar.
        """
        logger.info(f"Bar: {bar['start_time']}, Open: {bar['open']}, High: {bar['high']}, "
              f"Low: {bar['low']}, Close: {bar['close']}, Volume: {bar['volume']:0.2f}, VWAP: {bar['vwap']:0.2f}")


# Feed consumer
async def receive_data(uri):
    """
    Receives data from the WebSocket and processes it into 1-minute bars.
    """
    bar_aggregator = BarAggregator()

    async with websockets.connect(uri) as websocket:
        while True:
            try:
                message = await websocket.recv()
                msg_recv.inc()
                tick_time, price, volume = unpack_tick_msg(message)
                bar_aggregator.process_tick(tick_time, price, volume)
            except Exception as e:
                logging.error(f"Error processing message: {e}")
                continue

if __name__ == '__main__':

    signal.signal(signal.SIGINT, signal_handler)
    start_http_server(9002)
    asyncio.run(receive_data(URI))

