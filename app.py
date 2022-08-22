import asyncio
from order_book_stream import main_depth_stream
from price_stream import main_price_stream
import price_stream



async def main_app(ticker: str):
    tasks = [
        main_price_stream(ticker)
        ]
    await asyncio.gather(*tasks)
   

if __name__ == '__main__':
    # asyncio.run(main("MANAUSDT"))
    asyncio.run(main_app('MANAUSDT'))
