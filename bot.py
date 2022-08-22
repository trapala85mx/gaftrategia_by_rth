from binance.client import Client
from binance import AsyncClient, BinanceSocketManager
import pandas as pd
import numpy as np
import math
import asyncio
import os
from coin_data import data
from config import api_key, api_secret, telegram_token
import requests
from colorama import init, Fore, Style
import json
from datetime import datetime
from binance.exceptions import BinanceAPIException

init()


def enviar_señal(msg: str):
    """
    Envía señales a Telegram

    Args:
        msg (str): Mensaje que se envía a telegram
    """
    token = telegram_token
    bot_chatID = "1357832345"
    base = "https://api.telegram.org/bot"
    send_url = base + token + '/sendMessage'
    data = {
        'chat_id': bot_chatID,
        'text': msg
    }
    response = requests.post(send_url, json=data)
    return response


async def get_order_book(client: AsyncClient, symbol: str) -> dict:
    """
    Trae el libro de órdenes

    Args:
        client (AsyncClient): Cliente asíncrono de binance
        ticker (str): símbolo o tickersymbol de la moneda

    Returns:
        Un diccionario de acuerdo a la documentación de la API de Binance
        https://binance-docs.github.io/apidocs/futures/en/#order-book
    """
    try:
        client = Client(api_key=api_key, api_secret=api_secret)
        order_book = client.futures_order_book(symbol=symbol, limit=1000)
        return order_book
    except BinanceAPIException as bae:
        print(bae.response)
        print(f"Error al traer el libro de la moneda{symbol}")
        print(bae)


async def get_data_from_order_book(order_book: dict, trx: str) -> pd.DataFrame:
    """
    Extrae la información necesaria del libro de órdenes. Compras y Ventas

    Args:
        order_book (dict): Diccionario proveniente de la función get_order_book
        trx (str): tipo de transacción que vamos a extraer, ventas o compras o, por su
                    nombre en inglés, asks o bids, respectivamente

    Returns:
        pd.DataFrame: DataFrame con los datos, presentados de manera semejante a cómo
                      muestra Binance su libro de órdenes
    """
    df = order_book[trx]
    if trx == "bids":
        df = pd.DataFrame(df, columns=['precio', 'total_usdt'])
    else:
        df = pd.DataFrame(df, columns=['precio', 'total_usdt']).sort_index(
            axis=0, ascending=False)
    df[['precio', 'total_usdt']] = df[[
        'precio', 'total_usdt']].astype('float')
    return df


@DeprecationWarning
async def get_ventas_compras(order_book: dict) -> pd.DataFrame:
    """
    Función deprecada ya que se sustituyó por la anterior
    Función que retorna los dataframes de compras y ventas        

    Args:
        order_book (dict): recibe el diccionario del libro de órdenes
                           de la función get_order_book

    Returns:
        pd.DataFrame: Dos DataFrames, uno de ventas y otro de compras
    """
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
    """
    Función que obtiene los valores máximos y mínimos de un dataframe

    Args:
        df (pd.DataFrame): Recibe un DataFrame, ya sea de compras o ventas

    Returns:
        list: Una lista que contiene el valor mínimo y máximo del DataFrame
    """
    max = df['precio'].max()
    min = df['precio'].min()
    return [min, max]


def crear_rango(df: pd.DataFrame, delta: float, tipo: str) -> np.arange:
    """
    Función que genera los rangos en los que dividiremos el DataFrame

    Args:
        df (pd.DataFrame): El DataFrame del cual se va a crear el rango
        delta (float): La diferencia entre un número y otro del rango
        tipo (str): Indica si va a ser un rango de compra o venta dependiendo
                    el cual viene del tipo de DataFrame que enviémos

    Raises:
        ValueError: Debe escribirse "c" o "v" ya sea mayúscula o minúscula

    Returns:
        np.arange: Un objeto arange de numpy que contiene los valores iniciales
                   de cada rango de acuerdo a los datos ingresados
    """
    min, max = get_max_min(df)

    try:
        if tipo.lower() == 'c':
            inicio = (math.floor((min / delta))) * delta
            final = (math.ceil((max / delta))) * delta
            rango = np.arange(inicio, final+delta, delta)
        elif tipo.lower() == 'v':
            inicio = (math.floor((min / delta))) * delta
            final = (math.ceil((max / delta))) * delta
            rango = np.arange(inicio, final+delta, delta)
        else:
            raise ValueError(
                'Ingresa (c) si es rango de compra o (v) si es rango de venta')
    except ValueError as ve:
        print(ve)

    return rango


def get_shock_point(df: pd.DataFrame, rango: np.arange, tipo: str) -> float:
    """
    Obtiene un ShockPoint o punto de mayor acumulación de ventas o compras de
    un DataFrame

    Args:
        df (pd.DataFrame): El DataFrame del cual se extraerá el punto
        rango (np.arange): El rango en el cual se va a dividir el DataFrame
        tipo (str): Indica si es un DataFrame de compra o venta

    Returns:
        float: Retorna el punto de mayor acumulación del DataFrame agrupado
    """
    # Necesitamos saber qué mpultiplo estamos obteniendo porque en compras
    # debemos obtener el lado izquierdo para el múltiplo 2 mientras que
    # para ventas y primera vuelta de compras el derecho
    df_ag = df.groupby(pd.cut(df.precio, rango)).sum()

    if tipo == 'v':
        sp = df_ag['total_usdt'].idxmax().right

    elif tipo == 'c':
        sp = df_ag['total_usdt'].idxmax().left

    return sp


async def get_shock_points(ticker: str, ventas: pd.DataFrame, compras: pd.DataFrame) -> dict:
    """
    Función que obtiene los dos shock points de acuerdo a la estrategia del gafas

    Args:
        ticker (str): ticker symbol de la moneda
        ventas (pd.DataFrame): DatFrame de venta del que se extrae los Shock Points
        compras (pd.DataFrame): DataFrame de compra del que se extrae los Shock Points

    Returns:
        dict: Diccionario con los shock points de compra y venta del DataFrame
    """

    sp_v_1 = get_shock_point(ventas, crear_rango(
        ventas, data[ticker]['i1'], 'v'), 'v')
    sp_v_2 = get_shock_point(ventas, crear_rango(
        ventas, data[ticker]['i2'], 'v'), 'v')
    sp_c_1 = get_shock_point(compras, crear_rango(
        compras, data[ticker]['i1'], 'c'), 'c')
    sp_c_2 = get_shock_point(compras, crear_rango(
        compras, data[ticker]['i2'], 'c'), 'c')
    spv = [sp_v_1, sp_v_2]
    spc = [sp_c_1, sp_c_2]

    # print({
    #    'symbol': ticker,
    #    'ventas': spv,
    #    'compras': spc
    # })

    return {
        'symbol': ticker,
        'ventas': spv,
        'compras': spc
    }


async def analizar_shock_points(shock_points: dict, sl=0.2) -> bool:
    """
    Función que analiza los shock points cumplan con la separación de 1:2

    Args:
        shock_points (dict): Diccionario Son los puntos de venta y compra que tiene que analizar
        sl (float, optional): Es un respiro al precio, es un % porcentaje de separación entre el último Shock Point y el Stop Loss. Defaults to 0.2.

    Returns:
        bool: Lista de booleano que contiene si cumple o no para compras y ventas
    """    
    flag_venta = False
    flag_compra = False

    sp_v_m = shock_points['ventas'][0]
    sp_v_M = shock_points['ventas'][1]
    sp_c_m = shock_points['compras'][0]
    sp_c_M = shock_points['compras'][1]

    if (sp_v_m != sp_v_M) and (sp_c_m != sp_c_M) and (sp_v_M > sp_v_m) and (sp_c_M > sp_c_m):
        pct_ventas = round(((sp_v_M - sp_v_m) / sp_v_m) * 100, 2)
        pct_compras = round(((sp_c_M - sp_c_m) / sp_c_M) * 100, 2)
        pct_sep_1 = (((sp_c_M - sp_v_m) / sp_c_M) * -100)
        pct_sep_2 = (((sp_v_m - sp_c_M) / sp_v_m) * 100)
        pct_sep = round((pct_sep_1+pct_sep_2)/2, 2)

        if (pct_sep/(pct_ventas+sl)) >= 2:  # Aquí es pct_sep/(pct_ventas+SL)
            #print(f'Se puede vender en {ticker}')
            flag_venta = True
        if (pct_sep/(pct_compras+sl)) >= 2:  # Aquí es pct_sep/(pct_compras+SL)
            #print(f'Se puede comprar en {ticker}')
            flag_compra = True

    return flag_venta, flag_compra


def get_price(ticker):
    pass


async def run(ticker, client):
    while True:
        order_book = await get_order_book(client, symbol=ticker)

        ventas, compras = await asyncio.gather(
            get_data_from_order_book(order_book, 'asks'),
            get_data_from_order_book(order_book, 'bids')
        )

        shock_points = await get_shock_points(ticker, ventas, compras)

        signal_venta, signal_compra = await analizar_shock_points(shock_points)

        if signal_venta and signal_compra:
            print(shock_points['symbol'])
            msg = """
            ****************************************************
                 Señal de {}VENTA{} y {}COMPRA{} en {}
            ****************************************************
            2.- {} 
            1.- {}
            ------------------------------
            1.- {}
            2.- {}""".format(Fore.RED, Style.RESET_ALL, Fore.GREEN, Style.RESET_ALL, shock_points['symbol'], shock_points['ventas'][1], shock_points['ventas'][0], shock_points['compras'][1], shock_points['compras'][0])
            print(msg)
            # enviar_señal(
            #    f'señal de venta y compra en {ticker}\n{shock_points}')
            break
        if signal_venta and not signal_compra:
            print(shock_points['symbol'])
            #print(f'señal de venta en {ticker}\n{shock_points["ventas"]}')
            msg = """
            ****************************************
                 Señal de {}VENTA{} en {}
            ****************************************
            2.- {} 
            1.- {}
            ------------------------------
            1.- {}
            2.- {}""".format(Fore.RED, Style.RESET_ALL, ticker, shock_points['ventas'][1], shock_points['ventas'][0], shock_points['compras'][1], shock_points['compras'][0])
            print(msg)
            break
        if not signal_venta and signal_compra:
            print(shock_points['symbol'])
            #print(f"señal de compra en {ticker}\n {shock_points['compras']}")
            msg = """
            ****************************************
                 Señal de {}COMPRA{} en {}
            ****************************************
            2.- {} 
            1.- {}
            ------------------------------
            1.- {}
            2.- {}""".format(Fore.GREEN, Style.RESET_ALL, ticker, shock_points['ventas'][1], shock_points['ventas'][0], shock_points['compras'][1], shock_points['compras'][0])
            print(msg)
            break


async def main():
    with open("./coin_data.json", "r", encoding='utf-8') as f:
        tickers = json.load(f)
#    tickers = [
#    "ETHUSDT",
#    "EOSUSDT",
#    "XLMUSDT",
#    "XTZUSDT",
#    "BNBUSDT",
#    "ONTUSDT",
#    "QTUMUSDT",
#    "THETAUSDT",
#    "ZILUSDT",
#    "KNCUSDT",
#    "ZRXUSDT",
#    "OMGUSDT",
#    "SXPUSDT",
#    "BANDUSDT",
#    "RLCUSDT",
#    "MKRUSDT",
#    "DEFIUSDT",
#    "BALUSDT",
#    "RUNEUSDT",
#    "SUSHIUSDT",
#    "SRMUSDT",
#    "ICXUSDT",
#    "AVAXUSDT",
#    "ENJUSDT",
#    "TOMOUSDT",
#    "RENUSDT",
#    "AAVEUSDT",
#    "LRCUSDT",
#    "ALPHAUSDT",
#    "GRTUSDT",
#    "1INCHUSDT",
#    "SANDUSDT",
#    "BTSUSDT",
#    "LITUSDT",
#    "REEFUSDT",
#    "RVNUSDT",
#    "SFPUSDT",
#    "CHRUSDT",
#    "MANAUSDT",
#    "ALICEUSDT",
#    "LINAUSDT",
#    "DENTUSDT",
#    "CELRUSDT",
#    "HOTUSDT",
#    "OGNUSDT",
#    "DGBUSDT",
#    "1000SHIBUSDT",
#    "BTCDOMUSDT",
#    "ARUSDT",
#    "KLAYUSDT",
#    "LPTUSDT",
#    "ROSEUSDT",
#    "IMXUSDT",
#    "API3USDT",
#    "WOOUSDT",
#    "JASMYUSDT",
#    "GALUSDT",
#]
    try:
        client = await AsyncClient.create()
        #tickers = ["MANAUSDT"]
        tasks = []
        for t in tickers:
            tasks.append(asyncio.create_task(run(t, client)))
        await asyncio.gather(*tasks)

    except asyncio.TimeoutError as ate:
        print(ate)
    finally:
        await client.close_connection()


if __name__ == '__main__':
    asyncio.run(main())

'''
[
    "ETHUSDT",
    "EOSUSDT",
    "XLMUSDT",
    "XTZUSDT",
    "BNBUSDT",
    "ONTUSDT",
    "THETAUSDT",
    "KNCUSDT",
    "ZRXUSDT",
    "OMGUSDT",
    "SXPUSDT",
    "BANDUSDT",
    "RLCUSDT",
    "MKRUSDT",
    "BALUSDT",
    "SUSHIUSDT",
    "SRMUSDT",
    "ICXUSDT",
    "AVAXUSDT",
    "ENJUSDT",
    "TOMOUSDT",
    "RENUSDT",
    "AAVEUSDT",
    "LRCUSDT",
    "ALPHAUSDT",
    "GRTUSDT",
    "1INCHUSDT",
    "SANDUSDT",
    "BTSUSDT",
    "LITUSDT",
    "REEFUSDT",
    "RVNUSDT",
    "SFPUSDT",
    "CHRUSDT",
    "MANAUSDT",
    "ALICEUSDT",
    "LINAUSDT",
    "DENTUSDT",
    "CELRUSDT",
    "HOTUSDT",
    "OGNUSDT",
    "DGBUSDT",
    "1000SHIBUSDT",
    "BTCDOMUSDT",
    "ARUSDT",
    "LPTUSDT",
    "ROSEUSDT",
    "IMXUSDT",
    "API3USDT",
    "JASMYUSDT",
    "GALUSDT",
]
'''
