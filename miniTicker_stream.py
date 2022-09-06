import websockets
import asyncio
import json

from binance import BinanceSocketManager, AsyncClient
from binance.exceptions import BinanceAPIException
from order_book import OrderBook
#
#
async def main(symbol:str, order_book:OrderBook):
    client = await AsyncClient.create()
    bsm = BinanceSocketManager(client)
    miniTickerSocker = bsm.individual_symbol_ticker_futures_socket(symbol)
    while True:
        try:            
            async with miniTickerSocker as mts:
                msg = await mts.recv()
                #print(f"{'*' * 40}")
                #print(f"miniTicker de {symbol}")
                #print(msg)
                order_book.price = float(msg['data']['c'])
        except BinanceAPIException as bae:
            print(symbol)
            print(bae)

if __name__ == '__main__':
    asyncio.run(main("MANAUSDT"))