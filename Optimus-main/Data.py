import time
from websocket_manager import get_websocket_manager


MIN_VOLUME = 20_000_000


def on_ticker_data(data):
    if not isinstance(data, list):
        return

    for ticker in data:
        if ticker.get('e') == "24hrMiniTicker":
            symbol = ticker.get('s')
            price = ticker.get('c')
            volume = float(ticker.get('q', 0))

            if symbol.endswith('USDT') and volume > MIN_VOLUME:
                print(f"|symbol={symbol}|price={price}|volume={volume}|")


def main():
    manager = get_websocket_manager()
    manager.start()

    print("Waiting for WebSocket connection...")
    if not manager.wait_for_connection(timeout=30):
        print("Failed to connect to WebSocket")
        return

    print("Connected! Subscribing to mini ticker stream...")
    manager.subscribe("!miniTicker@arr", on_ticker_data)

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nStopping Data.py...")
        manager.stop()


if __name__ == "__main__":
    main()
