import websocket
import requests
import json
import binance.client as Client


MIN_VOLUME = 20_000_000

client = Client()

URL = f"wss://stream.binance.com:9443/ws/!miniTicker@arr"
def on_open(ws):
    print("connect to the socket")

def on_message(ws,message):
    data = json.loads(message)
    for ticker in data:
        if ticker.get('e') == "24hrMiniTicker":
           symbol = ticker.get('s')
           price = ticker.get('c')
           volume = float(ticker.get('q', 0))
              
           if any(symbol.endswith(suffix) for suffix in ['USDT']):
              if volume > 20_000_000:
                print(f"|symbol{symbol}|price={price}|volume={volume}|")


def on_close(close_status_code,close_msg):
    print("connection lost",close_status_code,close_msg)

def on_error(ws,error):
    print("websocket error",error)

ws = websocket.WebSocketApp(URL,
                            on_message=on_message,
                            on_error=on_error,
                            on_open=on_open,
                            on_close = on_close)

ws.run_forever()

def get_binance_client_info():
    pass


