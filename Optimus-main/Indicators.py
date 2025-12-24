import pandas as pd
import json
import websocket
import time
import threading
from binance.client import Client
import ta

client = Client()

with open('oversold_symbols.json','r') as f:
    data = json.load(f)

print(f"Loaded {len(data)} oversold symbols: {data}")

monitored_symbols = set(data)
ws = None
should_restart = False

def calculate_indicator(prices,volumes):
    df_calc = pd.DataFrame({
        'close' :prices,
        'volume' : volumes
    })
    ema_1 = ta.trend.EMAIndicator(close=df_calc['close'], window=9).ema_indicator().iloc[-1]
    ema_2 = ta.trend.EMAIndicator(close=df_calc['close'], window=21).ema_indicator().iloc[-1]

    obv_indicator = ta.volume.OnBalanceVolumeIndicator(close=df_calc['close'], volume=df_calc['volume'])
    obv = obv_indicator.on_balance_volume().iloc[-1]

    obv_values = obv_indicator.on_balance_volume()
    obv_sma = obv_values.rolling(window=20).mean().iloc[-1]


    return ema_1, ema_2,obv, obv_sma

df = pd.DataFrame(columns=['symbol', 'price', 'volume', 'timestamp','ema_1','ema_2','obv','obv_signal','ema_signal','total_signal'])

price_history = {}
volume_history = {}
obv_history = {}
obv_sma_history = {}
ema_1_history = {}
ema_2_history = {}

print("Loading price history...")
for symbol in data:
    try:
        prices = client.get_klines(symbol=symbol, interval='5m', limit=100)
        price_history[symbol] = [float(k[4]) for k in prices]
        volume_history[symbol] =  [float(k[5])for k in prices]

        ema_1, ema_2, obv, obv_sma = calculate_indicator(price_history[symbol], volume_history[symbol])
        obv_history[symbol] = [obv]
        obv_sma_history[symbol] = [obv_sma]
        ema_1_history[symbol] = [ema_1]
        ema_2_history[symbol] = [ema_2]

        print(f"Loaded {len(price_history[symbol])} candles for {symbol}")
        print(f"  Current Price: {price_history[symbol][-1]:.2f}")
        print(f"  EMA 9: {ema_1:.2f} | EMA 21: {ema_2:.2f}")
        print(f"  OBV: {obv:,.0f} | OBV SMA(20): {obv_sma:,.0f}")
    except Exception as e:
        print(f"Error loading {symbol}: {e}")
        price_history[symbol] = []
        volume_history[symbol] = []
        obv_history[symbol] = []
        obv_sma_history[symbol] = []
        ema_1_history[symbol] = []
        ema_2_history[symbol] = []

print(f"Price history loaded for {len(price_history)} symbols\n")

strong_buy_signals={}
strong_sell_signals={}
buy_signals={}
sell_signals={}

def obv_signals(obv,obv_sma):


    if len(obv) < 2 or len(obv_sma) < 2:
        return 0

    current_obv = obv[-1]
    current_sma = obv_sma[-1]

    previous_obv = obv[-2]
    previous_sma = obv_sma[-2]

    if previous_obv <=previous_sma and current_obv> current_sma:
        return 1

    elif previous_obv >= previous_sma and current_obv < current_sma:
        return -1

    if current_obv > current_sma:
        return 0.5

    elif current_obv < current_sma:
        return -0.5

    else:
        return 0

Ema_buy_signals = {}
Ema_sell_signals = {}

def ema_signals(ema_1_history, ema_2_history):
    if len(ema_1_history) < 2 or len(ema_2_history) < 2:
        return 0

    current_ema_1 = ema_1_history[-1]
    current_ema_2 = ema_2_history[-1]
    previous_ema_1 = ema_1_history[-2]
    previous_ema_2 = ema_2_history[-2]

    if previous_ema_1 <= previous_ema_2 and current_ema_1 > current_ema_2:
        return 1
    elif previous_ema_1 >= previous_ema_2 and current_ema_1 < current_ema_2:
        return -1

    return 0

def total_signals(obv_signal,ema_signal):
    # Combine ema and obv signals
    if obv_signal ==1 and ema_signal ==1:
        return 2 #very strong buy
    elif obv_signal == -1 and ema_signal == -1:
        return -2 #very stron sell
    elif (obv_signal ==1 and ema_signal ==0) or (obv_signal ==0.5 and ema_signal == 1):
        return 1  #strong buy
    elif (obv_signal == -1 and ema_signal ==0) or (obv_signal == -0.5 and ema_signal == -1):
        return -1 #strong sell
    #weak signals
    elif obv_signal + ema_signal >0:
        return 0.5
    elif obv_signal +ema_signal < 0:
        return -0.5

    else:
        return 0

def check_for_new_symbols():
    """Periodically check for new oversold symbols"""
    global monitored_symbols, should_restart, ws
    while True:
        time.sleep(5)
        try:
            with open('oversold_symbols.json', 'r') as f:
                current_symbols = set(json.load(f))

            new_symbols = current_symbols - monitored_symbols

            if new_symbols:
                print(f"\n{'='*80}")
                print(f"🔔 NEW OVERSOLD SYMBOLS DETECTED: {list(new_symbols)}")
                print(f"{'='*80}")

                for symbol in new_symbols:
                    try:
                        print(f"Loading history for new symbol: {symbol}")
                        prices = client.get_klines(symbol=symbol, interval='5m', limit=100)
                        price_history[symbol] = [float(k[4]) for k in prices]
                        volume_history[symbol] = [float(k[5]) for k in prices]

                        ema_1, ema_2, obv, obv_sma = calculate_indicator(price_history[symbol], volume_history[symbol])
                        obv_history[symbol] = [obv]
                        obv_sma_history[symbol] = [obv_sma]
                        ema_1_history[symbol] = [ema_1]
                        ema_2_history[symbol] = [ema_2]

                        print(f"✅ {symbol} added to monitoring")
                        print(f"  Current Price: {price_history[symbol][-1]:.2f}")
                        print(f"  EMA 9: {ema_1:.2f} | EMA 21: {ema_2:.2f}")
                        print(f"  OBV: {obv:,.0f} | OBV SMA(20): {obv_sma:,.0f}")
                    except Exception as e:
                        print(f"❌ Error loading {symbol}: {e}")
                        price_history[symbol] = []
                        volume_history[symbol] = []
                        obv_history[symbol] = []
                        obv_sma_history[symbol] = []
                        ema_1_history[symbol] = []
                        ema_2_history[symbol] = []

                monitored_symbols = current_symbols

                print(f"\n{'='*80}")
                print(f"🔄 Restarting WebSocket to monitor {len(monitored_symbols)} symbols...")
                print(f"{'='*80}\n")

                should_restart = True
                if ws:
                    ws.close()

        except Exception as e:
            print(f"Error checking for new symbols: {e}")


def on_open(ws):
    print("Connected to websocket")

def on_message(ws, message):
    global df, price_history, volume_history
    msg_data = json.loads(message)

    if 'data' in msg_data:
        msg_data = msg_data['data']

    if msg_data.get('e') == 'kline':
        k = msg_data['k']
        symbol = msg_data['s']
        close_price = float(k['c'])
        volume = float(k['v'])
        quote_volume = float(k['q'])
        is_closed = k['x']

        if symbol in price_history:

            temp_prices = price_history[symbol].copy()
            temp_volumes = volume_history[symbol].copy()
            temp_prices.append(close_price)
            temp_volumes.append(volume)

            ema_1, ema_2, obv, obv_sma = calculate_indicator(temp_prices, temp_volumes)

            if is_closed:
                price_history[symbol].append(close_price)
                volume_history[symbol].append(volume)
                if len(price_history[symbol]) > 100:
                    price_history[symbol].pop(0)
                    volume_history[symbol].pop(0)

            obv_history[symbol].append(obv)
            obv_sma_history[symbol].append(obv_sma)
            if len(obv_history[symbol]) > 100:
                obv_history[symbol].pop(0)
                obv_sma_history[symbol].pop(0)

            ema_1_history[symbol].append(ema_1)
            ema_2_history[symbol].append(ema_2)
            if len(ema_1_history[symbol]) > 100:
                ema_1_history[symbol].pop(0)
                ema_2_history[symbol].pop(0)

            signal = obv_signals(obv_history[symbol], obv_sma_history[symbol])

            if signal == 1:
                strong_buy_signals[symbol] = {'signal': 1, 'obv': obv, 'sma': obv_sma}
            elif signal == -1:
                strong_sell_signals[symbol] = {'signal': -1, 'obv': obv, 'sma': obv_sma}
            elif signal == 0.5:
                buy_signals[symbol] = {'signal': 0.5, 'obv': obv, 'sma': obv_sma}
            elif signal == -0.5:
                sell_signals[symbol] = {'signal': -0.5, 'obv': obv, 'sma': obv_sma}

            ema_signal = ema_signals(ema_1_history[symbol], ema_2_history[symbol])

            if ema_signal == 1:
                Ema_buy_signals[symbol] = {'ema_1': ema_1, 'ema_2': ema_2}
            elif ema_signal == -1:
                Ema_sell_signals[symbol] = {'ema_1': ema_1, 'ema_2': ema_2}

            combined_signal = total_signals(signal, ema_signal)

            timestamp = pd.Timestamp.now()

            df = df[df['symbol'] != symbol]


            new_row = pd.DataFrame({
                'symbol': [symbol],
                'price': [close_price],
                'volume': [quote_volume],
                'timestamp': [timestamp],
                'ema_1':[ema_1],
                'ema_2':[ema_2],
                'obv':[obv],
                'obv_signal':[signal],
                'ema_signal':[ema_signal],
                'total_signal':[combined_signal]
            })

            df = pd.concat([df, new_row], ignore_index=True)
            
            if combined_signal >=0.5:
                df[['symbol', 'price']].to_csv('snapshot.csv', index=False)
                
            
            print(f"\n{'='*80}")
            print(f"{symbol}: Price = {close_price}, Volume = {quote_volume:,.2f} USDT")
            print(f"EMA 9: {ema_1:.2f} | EMA 21: {ema_2:.2f}")
            print(f"OBV: {obv:,.0f} | OBV SMA(20): {obv_sma:,.0f}")
            print(f"-"*80)
            print(f"OBV Signal: {signal} | EMA Signal: {ema_signal}")
            print(f"TOTAL COMBINED SIGNAL: {combined_signal}")
            print(f"\n{'='*80}")


            if combined_signal == 2:
                print(f"🚀 VERY STRONG BUY - Both indicators bullish!")
            elif combined_signal == 1:
                print(f"📈 STRONG BUY - Good entry opportunity")
            elif combined_signal == 0.5:
                print(f"⬆️  WEAK BUY - Minor bullish signal")
            elif combined_signal == -0.5:
                print(f"⬇️  WEAK SELL - Minor bearish signal")
            elif combined_signal == -1:
                print(f"📉 STRONG SELL - Consider exit")
            elif combined_signal == -2:
                print(f"🔴 VERY STRONG SELL - Both indicators bearish!")
            else:
                print(f"➖ NEUTRAL - No clear signal")

            print(f"{'='*80}")
            print("\nUpdated DataFrame:")
            print(df.to_string(index=False))

def on_error(ws, error):
    print(f"Websocket error: {error}")

def on_close(ws, close_status_code, close_msg):
    print("Websocket closed")


checker_thread = threading.Thread(target=check_for_new_symbols, daemon=True)
checker_thread.start()

while True:
    try:
        should_restart = False

        streams = '/'.join([f"{s.lower()}@kline_5m" for s in monitored_symbols])
        URL = f"wss://stream.binance.com:9443/stream?streams={streams}"

        ws = websocket.WebSocketApp(URL,
                                    on_message=on_message,
                                    on_open=on_open,
                                    on_error=on_error,
                                    on_close=on_close)

        ws.run_forever()

        if not should_restart:
            break

    except KeyboardInterrupt:
        print("\n\nStopping Indicators.py...")
        break
    except Exception as e:
        print(f"WebSocket error: {e}")
        if not should_restart:
            print("Restarting in 5 seconds...")
            time.sleep(5)
