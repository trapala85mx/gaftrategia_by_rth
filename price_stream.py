from binance import BinanceSocketManager, AsyncClient
import asyncio
import websockets

coin_price = 0.0

#async def main_price_stream(ticker:str):
#    global coin_price
#    client = await AsyncClient.create()
#    bsm = BinanceSocketManager(client)
#    price_socket = bsm.individual_symbol_ticker_futures_socket(ticker)
#    
#    while True:
#        try:
#            async with price_socket as ps:
#                while True:
#                    res = await ps.recv()
#                    coin_price = float(res['data']['c'])
#                    #print(price_data['price'])
#                    yield coin_price
#        except Exception as e:
#            print(e)
#        
#        
#        await asyncio.sleep(10)

async def price_stream(ticker:str):
    socket = f"wss://fstream.binance.com/ws/{ticker.lower()}@miniTicker"
    while True:
        try:
            async with websockets.connect(socket) as ws:
                while True:
                    data = await ws.recv()
                    print(type(data))
                    print(data)
        except Exception as e:
            print(e)
        
        await asyncio.sleep(10)
        

if __name__ == '__main__':
    asyncio.run(price_stream("MANAUSDT"))