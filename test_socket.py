import websocket
import json

socket = "wss://stream.binance.com:9443/ws/btcusdt@kline_1m"

def on_message(ws, message):
    data = json.loads(message)
    candle = data['k']
    
    print({
        "timestamp": candle['t'],
        "open": candle['o'],
        "high": candle['h'],
        "low": candle['l'],
        "close": candle['c'],
        "volume": candle['v']
    })

ws = websocket.WebSocketApp(socket, on_message=on_message)
ws.run_forever()