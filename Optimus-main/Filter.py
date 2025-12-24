
from binance.client import Client
import ta
import pandas as pd
import websocket
import json


MIN_VOLUME = 100_000
RSI_PERIOD = 14
PAIR = 'USDT'

client = Client()

stats = client.get_ticker()
stats_map = {s['symbol']:float(s.get('quoteVolume',0))for s in stats}

info = client.get_exchange_info()

symbol = [
    s for s in info['symbols']
    if s['status'] == 'TRADING'
    and stats_map.get(s['symbol'],0) >= MIN_VOLUME and (s['symbol']).endswith(PAIR)
]

def get_historical_prices(symbol,interval="5m",limit=1000):
    klines = client.get_klines(symbol=symbol,interval=interval,limit=limit)

    close_price = [float(k[4])for k in klines]
    return close_price

def calcuate_rsi(prices,period=14):
    df = pd.DataFrame(prices,columns=['close'])
    rsi_indicator = ta.momentum.RSIIndicator(close=df['close'],window=period)
    rsi = rsi_indicator.rsi()
    return rsi.iloc[-1]

def generate_signals(rsi):
    if rsi <= 30:
        return "oversold"
    if rsi >= 70:
        return "overbought"
    else:
        return "HOLD"

def get_oversold_symbols():
    oversold_symbols = []
    print(f"Scanning {len(symbol)} symbols...")
    for s in symbol:
        symbol_name = s['symbol']
        prices = get_historical_prices(symbol_name, interval="5m", limit=100)
        rsi = calcuate_rsi(prices, period=RSI_PERIOD)
        signal = generate_signals(rsi)

        if signal == "oversold":
            oversold_symbols.append(symbol_name)
            print(f"{symbol_name}: RSI = {rsi:.2f}, Signal = {signal}")

    return oversold_symbols

if __name__ == "__main__":
    oversold_symbols = get_oversold_symbols()

    print(f"\nFound {len(oversold_symbols)} oversold symbols: {oversold_symbols}")

    with open('oversold_symbols.json','w') as f:
        json.dump(oversold_symbols,f)

    print("Starting WebSocket monitoring for ALL symbols...\n")
    print("Loading initial price history for all symbols...")

    all_symbols = [s['symbol'] for s in symbol]
    price_history = {}

    for s in symbol:
        symbol_name = s['symbol']
        prices = get_historical_prices(symbol_name, interval="5m", limit=100)
        price_history[symbol_name] = prices

    print(f"Loaded price history for {len(price_history)} symbols\n")

    def on_open(ws):
        print("WebSocket connected")

    def on_message(ws, message):
        data = json.loads(message)

        if 'data' in data:
            data = data['data']

        if data.get('e') == 'kline':
            k = data['k']
            symbol = data['s']
            close_price = float(k['c'])
            is_closed = k['x']

            if is_closed and symbol in price_history:
                price_history[symbol].append(close_price)
                if len(price_history[symbol]) > 100:
                    price_history[symbol].pop(0)

                if len(price_history[symbol]) >= RSI_PERIOD:
                    rsi = calcuate_rsi(price_history[symbol], period=RSI_PERIOD)
                    signal = generate_signals(rsi)

                    if signal == "oversold":
                        if symbol not in oversold_symbols:
                            oversold_symbols.append(symbol)
                            with open('oversold_symbols.json', 'w') as f:
                                json.dump(oversold_symbols, f)
                            print(f"ALERrT: {symbol}: Price = {close_price}, RSI = {rsi:.2f}, Signal = {signal}")
                    elif signal == "overbought":
                        print(f"ALERgT: {symbol}: Price = {close_price}, RSI = {rsi:.2f}, Signal = {signal}")


    def on_error(ws, error):
        print(f"WebSocket error: {error}")

    def on_close(ws, close_status_code, close_msg):
        print("WebSocket closed")

    streams = '/'.join([f"{s.lower()}@kline_5m" for s in all_symbols])
    URL = f"wss://stream.binance.com:9443/stream?streams={streams}"

    ws = websocket.WebSocketApp(URL,
                                on_message=on_message,
                                on_error=on_error,
                                on_open=on_open,
                                on_close=on_close)

    ws.run_forever()


