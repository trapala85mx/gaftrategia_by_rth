import websockets
import asyncio
import json
import pandas as pd
import numpy as np
import requests
import time
import math

from binance import AsyncClient, BinanceSocketManager
from coin_data import data

data_to_work = {
    'MANAUSDT':{
        'price': 0.0,
        'asks': None,
        'bids': None
    }
}

async def price_stream(ticker:str):
    client = await AsyncClient.create()
    bsm = BinanceSocketManager(client)
    price_socket = bsm.individual_symbol_ticker_futures_socket(ticker)
    
    while True:
        try:
            async with price_socket as ps:
                while True:
                    msg = await ps.recv()
                    asyncio.create_task(manage_data(msg, ticker))
                    #data_to_work['price'] = float(res['data']['c'])
                    #print(price)
            
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
    df_ag = df.groupby(pd.cut(df.precio, rango)).sum()

    if tipo == 'v':
        sp = df_ag['total_usdt'].idxmax().right

    elif tipo == 'c':
        sp = df_ag['total_usdt'].idxmax().left

    return sp


def get_shock_points(ticker, asks, bids) -> list:
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

async def manage_data(msg:dict , ticker:str ,asks = None, bids = None):
    global data_to_work
    #print(msg['data']['c'])
    price = float(msg['data']['c'])
    if price > 0:
        data_to_work[ticker]['price'] = price
    
    if asks:
        data_to_work['ticer']['asks'] = asks
    
    if bids:
        data_to_work['ticer']['bids'] = bids
    
    print(f"Precio: ${data_to_work[ticker]['price']}")
    print(f"Asks: \n{data_to_work[ticker]['asks']}")
    print(f"Bids: \n{data_to_work[ticker]['bids']}")
    



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
                    #print('inicio ciclo')

                    if pu_actual != u_anterior:
                        # print('Snapshot')
                        # print(f'{"+"*25}')
                        first_event = 1
                        last_update_id, asks, bids = get_snapshot(ticker)

                    else:

                        # print(f'{"+"*25}')
                        #print("pu = u anterior")
                        # print(f'{"+"*25}')
                        u = res['u']
                        U = res['U']

                        if U <= last_update_id and u >= last_update_id and first_event == 1:
                            first_event += 1
                            #print('Updating first event')
                            # print(f'{"+"*25}')
                            new_asks, new_bids = get_new_data_daframe(res)
                            asks, bids = update_order_book(
                                asks, bids, new_asks, new_bids)

                        if not u < last_update_id and first_event > 1:
                            first_event += 1
                            #print('updating next event')
                            # print(f'{"+"*25}')
                            new_asks, new_bids = get_new_data_daframe(res)
                            asks, bids = update_order_book(
                                asks, bids, new_asks, new_bids)
                            
                    data_to_work[ticker]['asks'] = asks
                    data_to_work[ticker]['bids'] = bids
                    u_anterior = res['u']
                    
        except Exception as e:
            print(e)

        await asyncio.sleep(10)


async def main(ticker: str):
    #global tickers
    #with open("./coin_data.json", "r", encoding='utf-8') as f:
    #    tickers = json.load(f)

    tasks = [
        price_stream(ticker),
        depth_stream(ticker),
        ]

    await asyncio.gather(*tasks)


if __name__ == "__main__":

    asyncio.run(main("MANAUSDT"))
