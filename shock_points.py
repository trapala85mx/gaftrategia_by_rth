import asyncio
import pandas as pd
import numpy as np
import math
from coin_data import data as d
from order_book import OrderBook


async def get_max_min(df: pd.DataFrame) -> list:
    max = df['price'].max()
    min = df['price'].min()
    return [max, min]


async def create_sell_range(max: float, min: float, mult: float) -> np.arange:
    start = (math.floor((min / mult))) * mult
    end = (math.ceil((max / mult))) * mult
    rango = np.arange(start, end+mult, mult)

    return rango


async def create_buy_range(max: float, min: float, mult: float) -> np.arange:
    start = (math.floor((min / mult))) * mult
    end = (math.ceil((max / mult))) * mult
    rango = np.arange(start, end+mult, mult)

    return rango


async def get_sell_point(df: pd.DataFrame, rango: np.arange) -> float:
    df_ag = df.groupby(pd.cut(df.price, rango)).sum()
    sp = df_ag['asks_qty'].idxmax().right
    return sp


async def get_buy_point(df: pd.DataFrame, rango: np.arange) -> float:
    df_ag = df.groupby(pd.cut(df.price, rango)).sum()
    df_ag = df_ag.drop(df_ag[df_ag['bids_qty'] == 0].index)
    sp = df_ag['bids_qty'].idxmax().left
    return sp


async def get_shock_points(order_book: OrderBook):
    tasks = [
        get_max_min(order_book.asks),
        get_max_min(order_book.bids)
    ]

    ventas_extremos, compras_extremos = await asyncio.gather(*tasks)

    tasks = [
        create_sell_range(
            ventas_extremos[0], ventas_extremos[1], d[order_book.symbol.upper()]['i1']),
        create_sell_range(
            ventas_extremos[0], ventas_extremos[1], d[order_book.symbol.upper()]['i2']),
        create_buy_range(
            compras_extremos[0], compras_extremos[1], d[order_book.symbol.upper()]['i1']),
        create_buy_range(
            compras_extremos[0], compras_extremos[1], d[order_book.symbol.upper()]['i2'])
    ]

    range_vm1, range_vm2, range_cm1, range_cm2 = await asyncio.gather(*tasks)

    tasks = [
        get_sell_point(order_book.asks, range_vm1),
        get_sell_point(order_book.asks, range_vm2),
        get_buy_point(order_book.bids, range_cm1),
        get_buy_point(order_book.bids, range_cm2)
    ]

    order_book.sp_v_m1, order_book.sp_v_m2, order_book.sp_c_m1, order_book.sp_c_m2 = await asyncio.gather(*tasks)
    
    #print(order_book.symbol)
    #print(order_book.sp_c_m1)
    #print(order_book.sp_c_m2)
