"""
WebSocket manager tests.
Run: pytest test_websocket.py -v
"""

import time
import pytest
from websocket_manager import get_websocket_manager, BinanceWebSocketManager


class TestWebSocketManager:
    """Test suite for BinanceWebSocketManager."""

    @pytest.fixture(autouse=True)
    def setup(self):
        """Reset singleton before each test."""
        BinanceWebSocketManager._instance = None
        self.manager = get_websocket_manager()
        self.messages_received = 0
        self.last_message = None
        yield
        if self.manager._should_run:
            self.manager.stop()

    def _on_message(self, data):
        """Callback to track received messages."""
        self.messages_received += 1
        self.last_message = data

    def test_singleton_pattern(self):
        """Manager should be a singleton."""
        manager1 = get_websocket_manager()
        manager2 = get_websocket_manager()
        assert manager1 is manager2

    def test_connection(self):
        """Should connect to Binance WebSocket."""
        self.manager.start()
        connected = self.manager.wait_for_connection(timeout=10)
        assert connected
        assert self.manager.is_connected()

    def test_subscribe(self):
        """Should register subscription."""
        self.manager.start()
        self.manager.wait_for_connection(timeout=10)
        self.manager.subscribe("btcusdt@kline_1m", self._on_message)
        assert self.manager.get_subscription_count() == 1

    def test_unsubscribe(self):
        """Should remove subscription."""
        self.manager.start()
        self.manager.wait_for_connection(timeout=10)
        self.manager.subscribe("btcusdt@kline_1m", self._on_message)
        self.manager.unsubscribe("btcusdt@kline_1m")
        assert self.manager.get_subscription_count() == 0

    def test_receive_messages(self):
        """Should receive messages from subscribed stream."""
        self.manager.start()
        self.manager.wait_for_connection(timeout=10)
        self.manager.subscribe("btcusdt@kline_1m", self._on_message)

        start = time.time()
        while self.messages_received < 1 and time.time() - start < 30:
            time.sleep(0.5)

        assert self.messages_received >= 1
        assert self.last_message is not None
        assert self.last_message.get('s') == 'BTCUSDT'

    def test_multiple_subscriptions(self):
        self.manager.start()
        self.manager.wait_for_connection(timeout=10)
        self.manager.subscribe("btcusdt@kline_1m", self._on_message)
        self.manager.subscribe("ethusdt@kline_1m", self._on_message)
        self.manager.subscribe("bnbusdt@kline_1m", self._on_message)
        assert self.manager.get_subscription_count() == 3

    def test_reconnection(self):
        """Should reconnect after disconnect."""
        self.manager.start()
        self.manager.wait_for_connection(timeout=10)
        self.manager.subscribe("btcusdt@kline_1m", self._on_message)

        self.manager._ws.close()
        time.sleep(1)

        reconnected = self.manager.wait_for_connection(timeout=10)
        assert reconnected

    def test_resubscribe_after_reconnect(self):
        """Should resubscribe and receive messages after reconnect."""
        self.manager.start()
        self.manager.wait_for_connection(timeout=10)
        self.manager.subscribe("btcusdt@kline_1m", self._on_message)

        start = time.time()
        while self.messages_received < 1 and time.time() - start < 30:
            time.sleep(0.5)

        messages_before = self.messages_received

        self.manager._ws.close()
        time.sleep(1)
        self.manager.wait_for_connection(timeout=10)

        start = time.time()
        while self.messages_received <= messages_before and time.time() - start < 30:
            time.sleep(0.5)

        assert self.messages_received > messages_before
