import asyncio

from binance import AsyncClient, Client
from order_book import OrderBook
from miniTicker_stream import main as start_mini_ticker_stream
from manage_data import manage_data
from depth_stream import start_depth_socket
#
#
symbols_accepted = set()
order_books = {}


async def filter_symbols(msg: list):
    filtered = [d for d in msg if d['symbol'][-4:] == "USDT" and float(
        d['quoteVolume']) > 200_000_000 and float(d['lastPrice']) <= 6]
    #filtered = [d for d in msg if d['symbol'][-4:] == "USDT" and float(d['quoteVolume'])>200_000_000]
    tickers = [d['symbol'] for d in filtered]

    return tickers


async def get_symbols():
    global symbols_accepted
    client = Client()

    res = client.futures_ticker()
    tickers = await filter_symbols(res)

    for t in tickers:
        symbols_accepted.add(t)


async def main():
    global symbols_accepted, order_books

    await get_symbols()
    print(symbols_accepted)

    tasks = []
    #symbols_accepted = ["MANAUSDT"]

    for t in symbols_accepted:
        order_books[t] = OrderBook(t)
        tasks.append(start_depth_socket(t, order_books[t])),
        tasks.append(start_mini_ticker_stream(t, order_books[t]))
        tasks.append(manage_data(order_books[t]))

    await asyncio.gather(*tasks)

if __name__ == '__main__':
    asyncio.run(main())
