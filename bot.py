from binance.client import Client
import pandas as pd
import numpy as np
import math
import time
import statistics
from coin_data import data
from config import api_key, api_secret


def get_order_book(ticker: str) -> dict:
    client = Client(api_key=api_key, api_secret=api_secret)
    order_book = client.futures_order_book(symbol=ticker, limit=1000)
    return order_book


def get_ventas_compras(order_book: dict) -> pd.DataFrame:
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


def get_shock_points(ticker: str, ventas: pd.DataFrame, compras: pd.DataFrame) -> dict:
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


def analizar_shock_points(shock_points: dict, sl=0.7) -> list:
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

    if (pct_sep/pct_ventas) >= 2.5:  # Aquí es pct_sep/(pct_ventas+SL)
        #print(f'Se puede vender en {ticker}')
        flag_venta = True
    if (pct_sep/pct_compras) >= 2.5:  # Aquí es pct_sep/(pct_compras+SL)
        #print(f'Se puede comprar en {ticker}')
        flag_compra = True

    return [flag_venta, flag_compra]


def main():
    ticker = 'MANAUSDT'
    flag = False
    while not flag:
        order_book = get_order_book(ticker=ticker)
        ventas, compras = get_ventas_compras(order_book)
        shock_points = get_shock_points(ticker, ventas, compras)
        venta, compra = analizar_shock_points(shock_points)
        print(shock_points)
        flag = venta and compra

    # Aqui forzosamente necesitamos asincronismo o paralelistmo
    # porque necesitamos que el manejo del análisis de ventas
    # y compras sea independiente y la gestión igual
    # Cuando encuentra venta que siga buscando compra independiente
    # no depender del ciclo

if __name__ == '__main__':
    main()
