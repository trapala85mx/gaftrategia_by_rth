import websockets
import asyncio
import json

from websockets.exceptions import ConnectionClosedError
from order_book import OrderBook


async def main(symbol:str, order_book: OrderBook):
    url = f"wss://fstream.binance.com/ws{symbol.lower()}@miniTicker"
    while True:
        try:
            subscribe = {
                "method": "SUBSCRIBE",
                "params": [f"{symbol.lower()}@miniTicker"],
                "id": 1,
            }

            
            async with websockets.connect(url, ping_interval = 300, ping_timeout=900) as websocket:
                await websocket.send(json.dumps(subscribe))
            
                while True:
                    msg = await websocket.recv()
                    msg = json.loads(msg)
                    if "e" in msg:
                        order_book.price = float(msg['c'])
                        order_book.vol24Hr = float(msg['q'])
                        
        except ConnectionClosedError as cc:
            print(cc)
            continue

        except KeyboardInterrupt as ki:
            print("Cierre por Ctrl+D")
            websocket.close()

if __name__ == '__main__':
    asyncio.run(main("BTCUSDT"))