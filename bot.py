from signal import signal
from typing import Awaitable
from binance.client import Client, AsyncClient
import pandas as pd
import numpy as np
import math
import asyncio
import os
from coin_data import data
from config import api_key, api_secret

price_data = {
    'precio': 0,
    'error': False
}


async def get_order_book(client: AsyncClient, symbol: str) -> dict:
    """_summary_

    Args:
        client (AsyncClient): _description_
        ticker (str): _description_

    Returns:
        A dictionary as shown in BinanceAPI Documentation
        https://binance-docs.github.io/apidocs/futures/en/#order-book
    """
    client = Client(api_key=api_key, api_secret=api_secret)
    order_book = client.futures_order_book(symbol=symbol, limit=1000)
    return order_book


async def get_data_from_order_book(order_book: dict, trx: str) -> pd.DataFrame:
    df = order_book[trx]
    if trx == "bids":
        df = pd.DataFrame(df, columns=['precio', 'total_usdt'])
    else:
        df = pd.DataFrame(df, columns=['precio', 'total_usdt']).sort_index(
            axis=0, ascending=False)
    df[['precio', 'total_usdt']] = df[[
        'precio', 'total_usdt']].astype('float')
    return df


async def get_ventas_compras(order_book: dict) -> pd.DataFrame:
    ventas = order_book['asks']
    ventas = pd.DataFrame(
        ventas, columns=['precio', 'total_usdt']).sort_index(axis=0, ascending=False)
    ventas[['precio', 'total_usdt']] = ventas[[
        'precio', 'total_usdt']].astype('float')
    compras = order_book['bids']
    compras = pd.DataFrame(compras, columns=['precio', 'total_usdt'])
    compras[['precio', 'total_usdt']] = compras[[
        'precio', 'total_usdt']].astype('float')
    return ventas, compras


def get_max_min(df: pd.DataFrame) -> list:
    max = df['precio'].max()
    min = df['precio'].min()
    return [min, max]


def crear_rango(df: pd.DataFrame, delta: float, tipo: str) -> np.arange:
    min, max = get_max_min(df)

    try:
        if tipo == 'c':
            inicio = (math.floor((min / delta))) * delta
            final = (math.ceil((max / delta))) * delta
            rango = np.arange(inicio, final+delta, delta)
        elif tipo == 'v':
            inicio = (math.floor((min / delta))) * delta
            final = (math.ceil((max / delta))) * delta
            rango = np.arange(inicio, final+delta, delta)
        else:
            raise ValueError(
                'Ingresa (c) si es rango de compra o (v) si es rango de venta')
    except ValueError as ve:
        print(ve)

    return rango


def get_shock_point(df: pd.DataFrame, rango: np.arange, tipo: str) -> list:
    df_ag = df.groupby(pd.cut(df.precio, rango)).sum()
    if tipo == 'v':
        sp = df_ag['total_usdt'].idxmax().right
    elif tipo == 'c':
        sp = df_ag['total_usdt'].idxmax().left

    return sp


async def get_shock_points(ticker: str, ventas: pd.DataFrame, compras: pd.DataFrame) -> dict:
    sp_v_1 = get_shock_point(ventas, crear_rango(
        ventas, data[ticker]['i1'], 'v'), 'v')
    sp_v_2 = get_shock_point(ventas, crear_rango(
        ventas, data[ticker]['i2'], 'v'), 'v')
    sp_c_1 = get_shock_point(compras, crear_rango(
        compras, data[ticker]['i1'], 'c'), 'c')
    sp_c_2 = get_shock_point(compras, crear_rango(
        compras, data[ticker]['i2'], 'c'), 'c')
    spv = sorted([sp_v_1, sp_v_2])
    spc = sorted([sp_c_1, sp_c_2])
    return {
        'ventas': spv,
        'compras': spc
    }


async def analizar_shock_points(shock_points: dict, sl=0.7) -> bool:
    flag_venta = False
    flag_compra = False

    sp_v_m = shock_points['ventas'][0]
    sp_v_M = shock_points['ventas'][1]
    sp_c_m = shock_points['compras'][0]
    sp_c_M = shock_points['compras'][1]

    pct_ventas = round(((sp_v_M - sp_v_m) / sp_v_m) * 100, 2)
    pct_compras = round(((sp_c_M - sp_c_m) / sp_c_M) * 100, 2)
    pct_sep_1 = (((sp_c_M - sp_v_m) / sp_c_M) * -100)
    pct_sep_2 = (((sp_v_m - sp_c_M) / sp_v_m) * 100)
    pct_sep = round((pct_sep_1+pct_sep_2)/2, 2)

    if (pct_sep/(pct_ventas+sl)) >= 2.5:  # Aquí es pct_sep/(pct_ventas+SL)
        #print(f'Se puede vender en {ticker}')
        flag_venta = True
    if (pct_sep/(pct_compras+sl)) >= 2.5:  # Aquí es pct_sep/(pct_compras+SL)
        #print(f'Se puede comprar en {ticker}')
        flag_compra = True

    return flag_venta, flag_compra


def get_price(ticker):
    pass


async def run(ticker, client):
    while True:
        order_book = await get_order_book(client, symbol=ticker)
        # ventas y compras son asíncronas y secuencial a order book
        # ventas = await get_data_from_order_book(order_book, 'asks')
        # compras = await get_data_from_order_book(order_book, 'bids')
        ventas, compras = await asyncio.gather(
            get_data_from_order_book(order_book, 'asks'),
            get_data_from_order_book(order_book, 'bids')
        )
        shock_points = await get_shock_points(ticker, ventas, compras)
        signal_venta, signal_compra = await analizar_shock_points(shock_points)
        print('ok')
        if signal_venta and signal_compra:
            print(f'señal de venta y compra en {ticker}\n{shock_points}')
            break
        if signal_venta and not signal_compra:
            print(f'señal de venta en {ticker}\n{shock_points["ventas"]}')
            break
        if not signal_venta and signal_compra:
            print(f"señal de compra en {ticker}\n {shock_points['compras']}")
            break


async def main():
    client = await AsyncClient.create()
    tickers = ["BTCUSDT", "ROSEUSDT", "MANAUSDT", "ADAUSDT", "MATICUSDT"]
    tasks = []
    for t in tickers:
        tasks.append(asyncio.create_task(run(t, client)))
    await asyncio.gather(*tasks)
    await client.close_connection()

'''
hasta quí tenemos un ciclo infinito que analiza el libro, trae los puntos
y nos dice si cumple la condición

Cuando cumpla alguna condición, ya sea que se de ambas entradas o solo una.
Lo que haremos es parar el ciclo, abrir un stream del precio y actualizar
el diccionario de data con el precio constantemente
'''

if __name__ == '__main__':
    #symbol = "MATICUSDT"
    asyncio.run(main())
    #loop = asyncio.get_event_loop()
    # loop.run_until_complete(main('MANAUSDT'))
