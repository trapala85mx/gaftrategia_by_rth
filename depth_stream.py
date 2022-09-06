import asyncio
import requests
import json

import pandas as pd
from binance import AsyncClient, BinanceSocketManager, Client
from binance.depthcache import FuturesDepthCacheManager, DepthCache
from binance.exceptions import BinanceAPIException
from shock_points import get_shock_points
from order_book import OrderBook
import pprint
#
#
first_event = 1
u_anterior = 0
#
#


async def orderDataFrame(df: pd.DataFrame, sort_by: str, columns: list) -> pd.DataFrame:
    if len(df.index) > 0:
        df.sort_values(sort_by, ascending=False, inplace=True)
        for c in columns:
            df[c] = df[c].astype('float64')
        return df
    else:
        return pd.DataFrame()
#
#
async def get_snapshot(symbol):
    url = f"https://fapi.binance.com/fapi/v1/depth?symbol={symbol.upper()}&limit=1000"
    try:
        res = requests.get(url)

        if res.status_code >= 200 and res.status_code < 300:
            res = json.loads(res.text)
            last_updated_id = res["lastUpdateId"]

            asks = pd.DataFrame(res['asks'], columns=[
                "price", "asks_qty"]).sort_values("price", ascending=False)

            bids = pd.DataFrame(res['bids'], columns=[
                "price", "bids_qty"]).sort_values("price", ascending=False)

            tasks = [
                orderDataFrame(asks, 'price', ['price', 'asks_qty']),
                orderDataFrame(bids, 'price', ['price', 'bids_qty'])
            ]

            asks, bids = await asyncio.gather(*tasks)

            return last_updated_id, asks, bids

        else:
            raise ValueError("No se pudo obtener el snapshot")
    except ValueError as ve:
        print(ve)
#
#
def swap_qty(orig_qty, new_qty) -> float:
        if orig_qty < 0 and new_qty >= 0:
            return new_qty

        elif orig_qty >= 0 and new_qty >= 0:
            return new_qty

        else:
            return orig_qty
#
#
async def combine_asks(orig_asks,newAsks) -> pd.DataFrame:   
        if len(newAsks.index) != 0:            
            asks = orig_asks.merge(newAsks, on='price', how='outer')
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
        
        return asks
#
#
async def combine_bids(orig_bids,newBids) -> pd.DataFrame:
        
        if len(newBids.index) != 0:
            bids = newBids.merge(orig_bids, on="price", how="outer")
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

        return bids
#
#
async def update_order_book(asks, bids, new_asks, new_bids):
    tasks = []
    tasks.append(orderDataFrame(new_asks, 'price', ['price','new_asks_qty']))
    tasks.append(orderDataFrame(new_bids, 'price', ['price','new_bids_qty']))

    new_asks, new_bids = await asyncio.gather(*tasks)
    
    tasks = [
        combine_asks(asks, new_asks),
        combine_bids(bids, new_bids)
    ]

    asks, bids = await asyncio.gather(*tasks)

    return asks, bids
#
#
async def start_depth_socket(symbol: str, order_book: dict, snap=None):
    
    global first_event, u_anterior
    client = await AsyncClient.create()
    bsm = BinanceSocketManager(client)
    futures_depth_socket = bsm.futures_depth_socket(symbol, depth="")
    
    while True:
        try:
            async with futures_depth_socket as fds:

                while True:
                    try:
                        msg = await fds.recv()
                        # print(msg['data']['e'])
                        pu_actual = msg['data']['pu']

                        if pu_actual != u_anterior:                            
                            last_updated_id, order_book.asks, order_book.bids = await get_snapshot(symbol)

                        else:
                            U = msg['data']['U']
                            u = msg['data']['u']

                            new_asks = pd.DataFrame(msg['data']['a'], columns=[
                                                    'price', 'new_asks_qty'])
                            new_bids = pd.DataFrame(msg['data']['b'], columns=[
                                                    'price', 'new_bids_qty'])
                            
                            if U <= last_updated_id and u >= last_updated_id and first_event == 1:                                
                                first_event += 1                                
                                order_book.asks, order_book.bids = await update_order_book(order_book.asks, order_book.bids, new_asks, new_bids)
                                await get_shock_points(order_book)

                            if not u < last_updated_id and first_event > 1:
                                first_event += 1
                                order_book.asks, order_book.bids = await update_order_book(order_book.asks, order_book.bids, new_asks, new_bids)
                                order_book.asks, order_book.bids = await update_order_book(order_book.asks, order_book.bids, new_asks, new_bids)
                                await get_shock_points(order_book)
                        
                        u_anterior = msg['data']['u']

                    except BinanceAPIException as bae:
                        print("No se pudo leer el libto de órdenes de ", symbol)

        except BinanceAPIException as bae:
            first_event = 1
            print(bae)
            continue

        except KeyboardInterrupt as kbi:
            break
#
#
if __name__ == '__main__':
    asyncio.run(start_depth_socket("MANAUSDT", {}))
