import time
import json
import ta
import pandas as pd
from binance.client import Client
from websocket_manager import get_websocket_manager


MIN_VOLUME = 100_000
RSI_PERIOD = 14
PAIR = 'USDT'

client = Client()

stats = client.get_ticker()
stats_map = {s['symbol']: float(s.get('quoteVolume', 0)) for s in stats}

info = client.get_exchange_info()

symbols = [
    s for s in info['symbols']
    if s['status'] == 'TRADING'
    and stats_map.get(s['symbol'], 0) >= MIN_VOLUME
    and s['symbol'].endswith(PAIR)
]

price_history = {}
oversold_symbols = []


def get_historical_prices(symbol, interval="5m", limit=100):
    klines = client.get_klines(symbol=symbol, interval=interval, limit=limit)
    close_prices = [float(k[4]) for k in klines]
    return close_prices


def calculate_rsi(prices, period=14):
    df = pd.DataFrame(prices, columns=['close'])
    rsi_indicator = ta.momentum.RSIIndicator(close=df['close'], window=period)
    rsi = rsi_indicator.rsi()
    return rsi.iloc[-1]


def generate_signals(rsi):
    if rsi <= 30:
        return "oversold"
    if rsi >= 70:
        return "overbought"
    return "HOLD"

def get_oversold_symbols():
    """Scan for oversold symbols - starts fresh each run."""
    found_oversold = []

    print(f"Scanning {len(symbols)} symbols...")

    for s in symbols:
        symbol_name = s['symbol']
        try:
            prices = get_historical_prices(symbol_name, interval="5m", limit=100)
            rsi = calculate_rsi(prices, period=RSI_PERIOD)
            signal = generate_signals(rsi)

            if signal == "oversold":
                found_oversold.append(symbol_name)
                with open('oversold_symbols.json', 'w') as f:
                    json.dump(found_oversold, f)
                print(f"{symbol_name}: RSI = {rsi:.2f}, Signal = {signal}")
        except Exception as e:
            print(f"Error scanning {symbol_name}: {e}")

    return found_oversold


def handle_kline_data(data):
    global oversold_symbols

    if data.get('e') != 'kline':
        return

    k = data['k']
    symbol = data['s']
    close_price = float(k['c'])
    is_closed = k['x']

    if not is_closed or symbol not in price_history:
        return

    price_history[symbol].append(close_price)
    if len(price_history[symbol]) > 100:
        price_history[symbol].pop(0)

    if len(price_history[symbol]) >= RSI_PERIOD:
        rsi = calculate_rsi(price_history[symbol], period=RSI_PERIOD)
        signal = generate_signals(rsi)

        if signal == "oversold":
            if symbol not in oversold_symbols:
                oversold_symbols.append(symbol)
                with open('oversold_symbols.json', 'w') as f:
                    json.dump(oversold_symbols, f)
                print(f"ALERT: {symbol}: Price = {close_price}, RSI = {rsi:.2f}, Signal = {signal}")
        elif signal == "overbought":
            print(f"ALERT: {symbol}: Price = {close_price}, RSI = {rsi:.2f}, Signal = {signal}")


def main():
    global oversold_symbols, price_history

    oversold_symbols = get_oversold_symbols()
    print(f"\nFound {len(oversold_symbols)} oversold symbols: {oversold_symbols}")

    print("\nStarting WebSocket monitoring for ALL symbols...")
    print("Loading initial price history for all symbols...")

    all_symbols = [s['symbol'] for s in symbols]
    for s in symbols:
        symbol_name = s['symbol']
        try:
            prices = get_historical_prices(symbol_name, interval="5m", limit=100)
            price_history[symbol_name] = prices
        except Exception as e:
            print(f"Error loading history for {symbol_name}: {e}")
            price_history[symbol_name] = []

    print(f"Loaded price history for {len(price_history)} symbols\n")

    # Initialize WebSocket manager
    manager = get_websocket_manager()
    manager.start()

    print("Waiting for WebSocket connection...")
    if not manager.wait_for_connection(timeout=30):
        print("Failed to connect to WebSocket")
        return

    print("Connected! Subscribing to kline streams...")

    # Subscribe to all symbol klines
    for symbol in all_symbols:
        stream = f"{symbol.lower()}@kline_5m"
        manager.subscribe(stream, handle_kline_data)

    print(f"Subscribed to {len(all_symbols)} kline streams")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nStopping Filter.py...")
        manager.stop()


if __name__ == "__main__":
    main()
