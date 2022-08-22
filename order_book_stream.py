import websockets
import asyncio
import json
import pandas as pd
import numpy as np
import requests
import time
import math

from binance import AsyncClient, BinanceSocketManager
from bot import analizar_shock_points
from coin_data import data
from colorama import init, Fore, Style


data_to_work = {}
# data_to_work = {
#    'MANAUSDT':{
#        'price': 0.0,
#        'asks': None,
#        'bids': None,
#        '24_hr_Volumne_USDT' : 0.0
#    },
#    'BTCUSDT':{
#        'price': 0.0,
#        'asks': None,
#        'bids': None,
#        '24_hr_Volumne_USDT' : 0.0
#    }
# }


async def price_stream(ticker: str):
    price_socket = f"wss://fstream.binance.com/ws/{ticker.lower()}@miniTicker"

    while True:
        try:
            async with websockets.connect(price_socket) as ps:
                while True:
                    msg = json.loads(await ps.recv())
                    asyncio.create_task(manage_data(msg, ticker))
                    #data_to_work['price'] = float(res['data']['c'])
                    # print(price)

        except Exception as e:
            print(e)

        await asyncio.sleep(10)


def get_snapshot(ticker):
    url = f"https://fapi.binance.com/fapi/v1/depth?symbol={ticker.upper()}&limit=1000"
    response = requests.get(url)
    ob = json.loads(response.text)
    last_update_id = ob["lastUpdateId"]

    bids = pd.DataFrame(ob['bids'], columns=['price', 'bids_qty'])
    bids['price'] = bids['price'].astype('float64')
    bids['bids_qty'] = bids['bids_qty'].astype('float64')
    bids.sort_values('price', inplace=True, ascending=False)

    asks = pd.DataFrame(ob['asks'], columns=['price', 'asks_qty'])
    asks['price'] = asks['price'].astype('float64')
    asks['asks_qty'] = asks['asks_qty'].astype('float64')
    asks.sort_values('price', inplace=True, ascending=False)

    return last_update_id, asks, bids


def get_new_data_daframe(res: dict):
    new_asks = pd.DataFrame(res['a'], columns=['price', 'new_asks_qty'])
    new_asks['new_asks_qty'] = new_asks['new_asks_qty'].astype('float64')
    new_asks['price'] = new_asks['price'].astype('float64')
    new_asks.sort_values('price', ascending=False, inplace=True)

    new_bids = pd.DataFrame(res['b'], columns=['price', 'new_bids_qty'])
    new_bids['new_bids_qty'] = new_bids['new_bids_qty'].astype('float64')
    new_bids['price'] = new_bids['price'].astype('float64')
    new_bids.sort_values('price', inplace=True, ascending=False)

    return new_asks, new_bids


def swap_qty(orig_qty, new_qty):
    if orig_qty < 0 and new_qty >= 0:
        return new_qty

    elif orig_qty >= 0 and new_qty >= 0:
        return new_qty

    else:
        return orig_qty


def update_order_book(orig_asks: pd.DataFrame, orig_bids: pd.DataFrame, new_asks: pd.DataFrame, new_bids: pd.DataFrame):
    if len(new_asks.index) != 0:
        asks = orig_asks.merge(new_asks, on='price', how='outer')
        asks.sort_values('price', inplace=True, ascending=False)
        asks.fillna(-1)
        asks['updated_qty'] = asks.apply(
            lambda x: swap_qty(x.asks_qty, x.new_asks_qty), axis=1)
        asks.drop(columns=['asks_qty', 'new_asks_qty'], axis=1, inplace=True)
        asks.rename(columns={'updated_qty': 'asks_qty'}, inplace=True)
        asks.sort_values('price', inplace=True, ascending=False)
        asks = asks.drop(asks[asks['asks_qty'] == 0].index)
        asks = asks.drop(asks[asks['asks_qty'].isnull()].index)
    else:
        asks = orig_asks

    if len(new_bids.index) != 0:
        bids = new_bids.merge(orig_bids, on="price", how="outer")
        bids.sort_values('price', inplace=True, ascending=False)
        bids = bids.fillna(-1)
        bids['updated_qty'] = bids.apply(
            lambda x: swap_qty(x.bids_qty, x.new_bids_qty), axis=1)
        bids.drop(columns=['bids_qty', 'new_bids_qty'], axis=1, inplace=True)
        bids.rename(columns={'updated_qty': 'bids_qty'}, inplace=True)
        bids.sort_values('price', inplace=True, ascending=False)
        bids = bids.drop(bids[bids['bids_qty'] == 0].index)
        bids = bids.drop(bids[bids['bids_qty'].isnull()].index)
    else:
        bids = orig_bids

    return asks, bids


def crear_rango(df: pd.DataFrame, delta: float, tipo: str) -> np.arange:
    max = df['price'].max()
    min = df['price'].min()
    if tipo.lower() == 'c':
        inicio = (math.floor((min / delta))) * delta
        final = (math.ceil((max / delta))) * delta
        rango = np.arange(inicio, final+delta, delta)
    elif tipo.lower() == 'v':
        inicio = (math.floor((min / delta))) * delta
        final = (math.ceil((max / delta))) * delta
        rango = np.arange(inicio, final+delta, delta)

    return rango


def get_shock_point(df: pd.DataFrame, rango: np.arange, tipo: str) -> float:
    df_ag = df.groupby(pd.cut(df.price, rango)).sum()

    if tipo == 'v':
        sp = df_ag['asks_qty'].idxmax().right

    elif tipo == 'c':
        sp = df_ag['bids_qty'].idxmax().left

    return sp


async def get_shock_points(ticker, asks, bids) -> list:
    sp_v_1 = get_shock_point(asks, crear_rango(
        asks, data[ticker]['i1'], 'v'), 'v')
    sp_v_2 = get_shock_point(asks, crear_rango(
        asks, data[ticker]['i2'], 'v'), 'v')
    sp_c_1 = get_shock_point(bids, crear_rango(
        bids, data[ticker]['i1'], 'c'), 'c')
    sp_c_2 = get_shock_point(bids, crear_rango(
        bids, data[ticker]['i2'], 'c'), 'c')

    spv = [sp_v_1, sp_v_2]
    spc = [sp_c_1, sp_c_2]

    return {
        'symbol': ticker,
        'ventas': spv,
        'compras': spc
    }


async def analizar_shock_points(ticker: str, shock_points: dict, sl=0.2):
    global data_to_work
    flag_venta = False
    flag_compra = False

    sp_v_m = shock_points['ventas'][0]
    sp_v_M = shock_points['ventas'][1]
    sp_c_m = shock_points['compras'][0]
    sp_c_M = shock_points['compras'][1]

    if (sp_v_m != sp_v_M) and (sp_c_m != sp_c_M) and (sp_v_M > sp_v_m) and (sp_c_M > sp_c_m) and (sp_c_m > 0):
        pct_ventas = round(((sp_v_M - sp_v_m) / sp_v_m) * 100, 2)
        pct_compras = round(((sp_c_M - sp_c_m) / sp_c_M) * 100, 2)
        pct_sep_1 = (((sp_c_M - sp_v_m) / sp_c_M) * -100)
        pct_sep_2 = (((sp_v_m - sp_c_M) / sp_v_m) * 100)
        pct_sep = round((pct_sep_1+pct_sep_2)/2, 2)

        if (pct_sep/(pct_ventas+sl)) >= 2:
            flag_venta = True
        if (pct_sep/(pct_compras+sl)) >= 2:
            flag_compra = True

    return flag_venta, flag_compra


def mostrar_mensaje(ticker: str, senal_venta: bool, senal_compra: bool, shock_points: dict):
    if senal_venta and senal_compra:
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
    if senal_venta and not senal_compra:
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
    if not senal_venta and senal_compra:
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


async def manage_data(msg: dict, ticker: str):
    #print(f"Entrando a manage data de {ticker}")
    # print(msg['data']['c'])
    price = float(msg['c'])
    vol24hr = float(msg['q'])
    asks = data_to_work[ticker]['asks']
    bids = data_to_work[ticker]['bids']
    data_to_work[ticker]['price'] = price
    data_to_work[ticker]['24_hr_Volumne_USDT'] = vol24hr
    
    try:
        if len(asks.index) > 0 and len(bids.index) > 0 and price > 0 and vol24hr >= 200_000_000:        
            shock_points = await get_shock_points(ticker, asks, bids)
            senal_venta, senal_compra = await analizar_shock_points(ticker, shock_points)
            
            mensaje = mostrar_mensaje(
                ticker, senal_venta, senal_compra, shock_points)
            if mensaje:
                print(mensaje)

       
    except Exception as e:
        pass


async def depth_stream(ticker: str):
    global data_to_work
    socket = f"wss://fstream.binance.com/stream?streams={ticker.lower()}@depth"
    u_anterior = 0
    first_event = 0
    while True:
        try:
            async with websockets.connect(socket) as ws:
                while True:
                    res = (json.loads(await ws.recv()))["data"]
                    pu_actual = res["pu"]

                    if pu_actual != u_anterior:
                        first_event = 1
                        last_update_id, asks, bids = get_snapshot(ticker)

                    else:

                        u = res['u']
                        U = res['U']

                        if U <= last_update_id and u >= last_update_id and first_event == 1:
                            first_event += 1
                            new_asks, new_bids = get_new_data_daframe(res)
                            asks, bids = update_order_book(
                                asks, bids, new_asks, new_bids)

                        if not u < last_update_id and first_event > 1:
                            first_event += 1
                            new_asks, new_bids = get_new_data_daframe(res)
                            asks, bids = update_order_book(
                                asks, bids, new_asks, new_bids)

                    data_to_work[ticker]['asks'] = asks
                    data_to_work[ticker]['bids'] = bids

                    u_anterior = res['u']

        except Exception as e:
            print(e)

        await asyncio.sleep(10)


async def main(tickers: list):
    global data_to_work

    
    
    for t in tickers:
        data_to_work.update(
            {
                t : {
                    'price': 0.0,
                    'asks': None,
                    'bids': None,
                    '24_hr_Volumne_USDT': 0.0,
                    'keep_looking': True
                }
            }
        )
            
    tasks = []
    for t in tickers:
        tasks.append(price_stream(t))
        tasks.append(depth_stream(t))
    # tasks = [
    #    price_stream(ticker),
    #    depth_stream(ticker),
    #    ]

    await asyncio.gather(*tasks)


if __name__ == "__main__":
    init()
    try:
        with open("./coin_data.json", "r", encoding='utf-8') as f:
            tickers = json.load(f)
        
        asyncio.run(main(tickers))
    except Exception as e:
        print(e)