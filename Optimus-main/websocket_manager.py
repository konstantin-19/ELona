import websocket
import json
import threading
import logging
from typing import Callable, Dict, List, Optional
from dataclasses import dataclass, field

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('BinanceWebSocketManager')


@dataclass
class StreamSubscription:

    stream_name: str
    callbacks: List[Callable[[dict], None]] = field(default_factory=list)


class BinanceWebSocketManager:
    _instance: Optional['BinanceWebSocketManager'] = None
    _lock: threading.Lock = threading.Lock()

    BASE_URL = "wss://stream.binance.com:9443/ws"

    def __new__(cls) -> 'BinanceWebSocketManager':
        with cls._lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
                cls._instance._initialized = False
            return cls._instance

    def __init__(self):
        if self._initialized:
            return

        self._ws: Optional[websocket.WebSocketApp] = None
        self._ws_thread: Optional[threading.Thread] = None
        self._connected: threading.Event = threading.Event()
        self._should_run: bool = False

        self._subscriptions_lock: threading.RLock = threading.RLock()
        self._subscriptions: Dict[str, StreamSubscription] = {}

        self._request_id: int = 0
        self._request_id_lock: threading.Lock = threading.Lock()

        self._initialized = True

    def start(self) -> None:
        if self._should_run:
            logger.warning("WebSocket manager already running")
            return

        self._should_run = True
        self._ws_thread = threading.Thread(target=self._connection_loop, daemon=True)
        self._ws_thread.start()
        logger.info("WebSocket manager started")

    def stop(self) -> None:
        self._should_run = False
        if self._ws:
            self._ws.close()
        if self._ws_thread and self._ws_thread.is_alive():
            self._ws_thread.join(timeout=5.0)
        self._connected.clear()
        logger.info("WebSocket manager stopped")

    def subscribe(self, stream: str, callback: Callable[[dict], None]) -> None:
        stream = stream.lower()

        with self._subscriptions_lock:
            if stream in self._subscriptions:
                if callback not in self._subscriptions[stream].callbacks:
                    self._subscriptions[stream].callbacks.append(callback)
                    logger.debug(f"Added callback to existing stream: {stream}")
            else:
                self._subscriptions[stream] = StreamSubscription(
                    stream_name=stream,
                    callbacks=[callback]
                )
                logger.info(f"Subscribing to new stream: {stream}")
                if self._connected.is_set() and self._ws:
                    self._send_subscribe([stream])

    def unsubscribe(self, stream: str, callback: Optional[Callable] = None) -> None:

        stream = stream.lower()

        with self._subscriptions_lock:
            if stream not in self._subscriptions:
                logger.warning(f"Stream not subscribed: {stream}")
                return

            if callback is not None:
                sub = self._subscriptions[stream]
                if callback in sub.callbacks:
                    sub.callbacks.remove(callback)
                    logger.debug(f"Removed callback from stream: {stream}")
                if not sub.callbacks:
                    del self._subscriptions[stream]
                    logger.info(f"Unsubscribing from stream: {stream}")
                    if self._connected.is_set() and self._ws:
                        self._send_unsubscribe([stream])
            else:
                del self._subscriptions[stream]
                logger.info(f"Unsubscribing from stream: {stream}")
                if self._connected.is_set() and self._ws:
                    self._send_unsubscribe([stream])

    def is_connected(self) -> bool:
        return self._connected.is_set()

    def wait_for_connection(self, timeout: float = 30.0) -> bool:
        return self._connected.wait(timeout)

    def get_subscription_count(self) -> int:
        with self._subscriptions_lock:
            return len(self._subscriptions)

    def _get_next_request_id(self) -> int:
        with self._request_id_lock:
            self._request_id += 1
            return self._request_id

    def _connection_loop(self) -> None:
        while self._should_run:
            try:
                self._connected.clear()

                self._ws = websocket.WebSocketApp(
                    self.BASE_URL,
                    on_open=self._on_open,
                    on_message=self._on_message,
                    on_error=self._on_error,
                    on_close=self._on_close
                )

                logger.info(f"Connecting to {self.BASE_URL}")
                self._ws.run_forever(ping_interval=30, ping_timeout=10)

            except Exception as e:
                logger.error(f"Connection error: {e}")

            if self._should_run:
                logger.info("Connection lost. Reconnecting immediately...")

    def _on_open(self, ws) -> None:
        logger.info("WebSocket connected")
        self._connected.set()

        with self._subscriptions_lock:
            streams = list(self._subscriptions.keys())

        if streams:
            logger.info(f"Resubscribing to {len(streams)} streams after reconnect")
            for i in range(0, len(streams), 200):
                batch = streams[i:i + 200]
                self._send_subscribe(batch)

    def _on_message(self, ws, message: str) -> None:
        try:
            data = json.loads(message)

            if 'result' in data and 'id' in data:
                if data['result'] is None:
                    logger.debug(f"Subscription confirmed for request {data['id']}")
                else:
                    logger.warning(f"Subscription error: {data}")
                return

            if isinstance(data, list):
                self._dispatch_array_message(data)
            else:
                self._dispatch_message(data)

        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse message: {e}")
        except Exception as e:
            logger.error(f"Error processing message: {e}")

    def _on_error(self, ws, error) -> None:
        logger.error(f"WebSocket error: {error}")

    def _on_close(self, ws, close_status_code, close_msg) -> None:
        self._connected.clear()
        logger.warning(f"WebSocket closed: {close_status_code} - {close_msg}")

    def _send_subscribe(self, streams: List[str]) -> None:
        if not streams or not self._ws:
            return

        request = {
            "method": "SUBSCRIBE",
            "params": streams,
            "id": self._get_next_request_id()
        }

        try:
            self._ws.send(json.dumps(request))
            logger.info(f"SUBSCRIBE sent for {len(streams)} streams: {streams[:3]}{'...' if len(streams) > 3 else ''}")
        except Exception as e:
            logger.error(f"Failed to send SUBSCRIBE: {e}")

    def _send_unsubscribe(self, streams: List[str]) -> None:
        if not streams or not self._ws:
            return

        request = {
            "method": "UNSUBSCRIBE",
            "params": streams,
            "id": self._get_next_request_id()
        }

        try:
            self._ws.send(json.dumps(request))
            logger.info(f"UNSUBSCRIBE sent for {len(streams)} streams")
        except Exception as e:
            logger.error(f"Failed to send UNSUBSCRIBE: {e}")

    def _dispatch_message(self, data: dict) -> None:
        stream = self._identify_stream(data)
        if not stream:
            return

        with self._subscriptions_lock:
            if stream in self._subscriptions:
                callbacks = self._subscriptions[stream].callbacks.copy()
            else:
                callbacks = []

        for callback in callbacks:
            try:
                callback(data)
            except Exception as e:
                logger.error(f"Callback error for {stream}: {e}")

    def _dispatch_array_message(self, data_list: list) -> None:
        stream = "!miniticker@arr"

        with self._subscriptions_lock:
            if stream in self._subscriptions:
                callbacks = self._subscriptions[stream].callbacks.copy()
            else:
                callbacks = []

        for callback in callbacks:
            try:
                callback(data_list)
            except Exception as e:
                logger.error(f"Callback error for {stream}: {e}")

    def _identify_stream(self, data: dict) -> Optional[str]:

        event_type = data.get('e')

        if event_type == 'kline':
            symbol = data.get('s', '').lower()
            interval = data.get('k', {}).get('i', '')
            return f"{symbol}@kline_{interval}"

        elif event_type == '24hrMiniTicker':
            symbol = data.get('s', '').lower()
            return f"{symbol}@miniticker"

        elif event_type == 'trade':
            symbol = data.get('s', '').lower()
            return f"{symbol}@trade"

        elif event_type == 'aggTrade':
            symbol = data.get('s', '').lower()
            return f"{symbol}@aggtrade"

        elif event_type == 'depth':
            symbol = data.get('s', '').lower()
            return f"{symbol}@depth"

        return None


def get_websocket_manager() -> BinanceWebSocketManager:
    return BinanceWebSocketManager()
