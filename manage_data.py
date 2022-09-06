import asyncio
import pandas as pd
import numpy as np
from order_book import OrderBook
from coin_data import data as d
#
#
async def rsi14():
    pass
#
#
async def bollinger_bands():
    pass
#
#
async def analyze_sell_points(order_book: OrderBook, sl: float) -> list:
    nearest = np.max([order_book.sp_c_m1, order_book.sp_c_m2])
    if (order_book.sp_v_m1 > order_book.sp_v_m2):
        pct_ventas = round(
            (abs(order_book.sp_v_m2 - order_book.sp_v_m1)/order_book.sp_v_m2)*100, 2)
        pct_sep_1 = round(
            (abs(order_book.sp_v_m2 - nearest)/order_book.sp_v_m2)*100, 2)
        pct_sep_2 = round((abs(nearest - order_book.sp_v_m2)/nearest)*100, 2)
        pct_sep = round((pct_sep_1+pct_sep_2)/2, 2)

        if (pct_sep/(pct_ventas+sl)) >= 2:
            return [order_book.sp_v_m1, order_book.sp_v_m2, nearest] # [punto_mas_lejano, punto_mas_cercano, tp]

    return [0.0, 0.0]  # [punto_mas_lejano, punto_mas_cercano]
#
#
async def analyze_buy_points(order_book: OrderBook, sl: float) -> float:

    nearest = np.min([order_book.sp_v_m1, order_book.sp_v_m2])

    if (order_book.sp_c_m2 > order_book.sp_c_m1) and (order_book.sp_c_m1 > 0):
        pct_compras = round(
            (abs(order_book.sp_c_m2 - order_book.sp_c_m1)/order_book.sp_c_m2)*100, 2)
        pct_sep_1 = round((abs(order_book.sp_c_m2 - nearest)/order_book.sp_c_m2)*100, 2)
        pct_sep_2 = round((abs(nearest - order_book.sp_c_m2)/nearest)*100, 2)
        pct_sep = round((pct_sep_1+pct_sep_2)/2, 2)

        if (pct_sep/(pct_compras+sl)) >= 2:
            return [order_book.sp_c_m1, order_book.sp_c_m2, nearest]

    return [0.0, 0.0]
#
#
async def signal(sell_data_points:list, buy_data_points:list, order_book:OrderBook):
    pass
#
#
async def manage_data(order_book: OrderBook):
    while True:

        if order_book.price > 0 and order_book.sp_v_m1 > 0:
            sl = 0.2 if "sl" not in d[order_book.symbol] else d[order_book.symbol]['sl']
            tasks = [
                analyze_sell_points(order_book, sl),
                analyze_buy_points(order_book, sl)
            ]

            sell_data_points, buy_data_points = await asyncio.gather(*tasks)
            #print(order_book.symbol)
            #print(order_book.sp_c_m1)
            #print(order_book.sp_c_m2)
            #print(buy_data_points)
            #signal = await signal(sell_data_points, buy_data_points, order_book)
            # print(order_book.symbol)
            # print(order_book.sp_v_m1)
            # print(order_book.sp_v_m2)
            # print(order_book.sp_c_m2)
            # print(order_book.sp_c_m1)
        await asyncio.sleep(0)
