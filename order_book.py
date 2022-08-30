import requests
import json
import pandas as pd
import numpy as np
import asyncio
import math
import os

from coin_data import data as d

class OrderBook:

    # ------------------------------------------------------------ CONSTRUCTOR(S)
    def __init__(self, symbol: str) -> None:
        self._symbol = symbol
        self._price = 0.0
        self._vol24Hr = 0.0
        self._asks = pd.DataFrame()
        self._bids = pd.DataFrame()
        self._uAnterior = 0
        self._newAsks = pd.DataFrame()
        self._newBids = pd.DataFrame()
        self._sell_points = None
        self._buy_points = None
        self._sp_v_m1 = 0.0
        self._sp_v_m2 = 0.0
        self._sp_c_m1 = 0.0
        self._sp_c_m2 = 0.0

    # ---------------------------------------------------------------------- METHODS USED INSIDE CLASS
    async def _orderDataFrame(self, df:pd.DataFrame, sort_by:str, columns:list) -> pd.DataFrame:
        if len(df.index) > 0:
            df.sort_values(sort_by, ascending=False, inplace=True)
            for c in columns:
                df[c] = df[c].astype('float64')
            return df    
        else:
            return pd.DataFrame()

    def _swap_qty(self,orig_qty, new_qty) -> float:
        if orig_qty < 0 and new_qty >= 0:
            return new_qty

        elif orig_qty >= 0 and new_qty >= 0:
            return new_qty

        else:
            return orig_qty

    async def _combine_asks(self,orig_asks,newAsks) -> pd.DataFrame:
        #print(orig_asks.info())
        #print(newAsks.info())
        
        if len(newAsks.index) != 0:            
            asks = orig_asks.merge(newAsks, on='price', how='outer')
            asks.sort_values('price', inplace=True, ascending=False)
            asks.fillna(-1)
            asks['updated_qty'] = asks.apply(
                lambda x: self._swap_qty(x.asks_qty, x.new_asks_qty), axis=1)
            asks.drop(columns=['asks_qty', 'new_asks_qty'], axis=1, inplace=True)
            asks.rename(columns={'updated_qty': 'asks_qty'}, inplace=True)
            asks.sort_values('price', inplace=True, ascending=False)
            asks = asks.drop(asks[asks['asks_qty'] == 0].index)
            asks = asks.drop(asks[asks['asks_qty'].isnull()].index)
        else:
            asks = orig_asks
        
        #print(asks)
        #print(f"{'*'*30}")
        return asks

    async def _combine_bids(self,orig_bids,newBids) -> pd.DataFrame:
        #print(orig_bids.info())
        #print(newBids.info())
        
        if len(newBids.index) != 0:
            bids = newBids.merge(orig_bids, on="price", how="outer")
            bids.sort_values('price', inplace=True, ascending=False)
            bids = bids.fillna(-1)
            bids['updated_qty'] = bids.apply(
                lambda x: self._swap_qty(x.bids_qty, x.new_bids_qty), axis=1)
            bids.drop(columns=['bids_qty', 'new_bids_qty'], axis=1, inplace=True)
            bids.rename(columns={'updated_qty': 'bids_qty'}, inplace=True)
            bids.sort_values('price', inplace=True, ascending=False)
            bids = bids.drop(bids[bids['bids_qty'] == 0].index)
            bids = bids.drop(bids[bids['bids_qty'].isnull()].index)
            
        else:
            bids = orig_bids
        
        #print(bids)
        #print(f"{'*'*30}")
        return bids

    async def _create_sell_range(self, max:float, min:float, mult:float) -> np.arange:
        # recibimos el máximo, mínimo y el múltiplo
        start = (math.floor((min / mult))) * mult
        end = (math.ceil((max / mult))) * mult
        rango = np.arange(start, end+mult, mult)
        
        return rango

    async def _create_buy_range(self, max:float, min:float, mult:float) -> np.arange:
        start = (math.floor((min / mult))) * mult
        end = (math.ceil((max / mult))) * mult
        rango = np.arange(start, end+mult, mult)

        return rango

    async def _get_max_min(self, df:pd.DataFrame) -> list:
        max = df['price'].max()
        min = df['price'].min()
        return [max, min]

    async def _get__sell_point(self, df:pd.DataFrame, rango:np.arange) -> float:
        df_ag = df.groupby(pd.cut(df.price, rango)).sum()
        sp = df_ag['asks_qty'].idxmax().right
        return sp
    
    async def _get__buy_point(self, df:pd.DataFrame, rango:np.arange) -> float:
        df_ag = df.groupby(pd.cut(df.price, rango)).sum()
        sp = df_ag['bids_qty'].idxmax().left
        return sp

    # ---------------------------------------------------------------------- PUBLIC METHODS
    async def get_shock_points(self) -> list:
        # 1. Obtener el punto más fuerte de compras y ventas para cada múltiplo. 
        #   Para esto debemos, primero, crear los rangos respecto a cada múltiplo
        #   como la lógica es diferente sea compra o venta, creamos dos funciones para
        #   así hacerlo asíncrono.
        # 2. Lo que necesitan ambos métodos, de base son los puntos máximos y mínimos
        #   del Dataframe
        tasks = [
            self._get_max_min(self.asks),
            self._get_max_min(self.bids)
        ]

        ventas_extremos, compras_extremos = await asyncio.gather(*tasks)
        #print(ventas_extremos) # [max, min]
        #print(compras_extremos) # [max, min]

        # 3. Para crear el rango necesitamos indicar el múltiplo, que obtenemos del archivo
        #   coind_data.py. Como son independientes, podemos usar asincronismo
        tasks = [
            self._create_sell_range(ventas_extremos[0], ventas_extremos[1], d[self.symbol.upper()]['i1']),
            self._create_sell_range(ventas_extremos[0], ventas_extremos[1], d[self.symbol.upper()]['i2']),
            self._create_buy_range(compras_extremos[0], compras_extremos[1], d[self.symbol.upper()]['i1']),
            self._create_buy_range(compras_extremos[0], compras_extremos[1], d[self.symbol.upper()]['i2'])
        ]

        range_v1, range_v2, range_c1, range_c2 = await asyncio.gather(*tasks)
        
        # 4. Ya con el rango, obtenemos el punto más fuerte del libro (ventas o compras) con cada rango
        #   si es compra o venta determina qué lado del rango se va a tomar por lo que haremos dos
        #   métodos y, a su vez, asíncronos para que se ejecuten todos
        tasks = [
            self._get__sell_point(self._asks, range_v1),
            self._get__sell_point(self._asks, range_v2),
            self._get__buy_point(self._bids, range_c1),
            self._get__buy_point(self._bids, range_c2)
        ]

        #sp_v1 esle shock point de ventas con el múltiplo 1, es decir, debe ser el más alejado
        #sp_v2 esle shock point de ventas con el múltiplo 2, es decir, debe ser el más cercano
        #los maximos son los más alejados y los mínimos los más cercanos al precio
        self._sp_v_m1, self._sp_v_m2, self._sp_c_m1, self._sp_c_m2 = await asyncio.gather(*tasks) 
        
        return self

    async def update(self):
        
        #print(self._newBids.info())
        tasks = [
            self._orderDataFrame(self._newAsks, 'price', ['price','new_asks_qty']),
            self._orderDataFrame(self._newBids, 'price', ['price','new_bids_qty'])
        ]
        self._newAsks, self._newBids = await asyncio.gather(*tasks)
        
        #print(self._newBids.info())

        # Hasta aqui ok
        
        tasks = [
            self._combine_asks(self._asks, self._newAsks),
            self._combine_bids(self._bids, self._newBids)
        ]
        
        self._asks, self._bids = await asyncio.gather(*tasks)
        #print(self._asks)
        #print(self._bids)
        #print(f"{'*'*30}")
        # hasta aquí tengo todo los datos para poder analizar y hacer los merge demás
        #print(self)

        return self

    async def get_snapshot(self):
        url = f"https://fapi.binance.com/fapi/v1/depth?symbol={self._symbol.upper()}&limit=1000"
        try:
            res = requests.get(url)

            if res.status_code == 200:
                res = json.loads(res.text)
                last_updated_id = res["lastUpdateId"]

                asks = pd.DataFrame(res['asks'], columns=[
                    "price", "asks_qty"]).sort_values("price", ascending=False)
                

                bids = pd.DataFrame(res['bids'], columns=[
                    "price", "bids_qty"]).sort_values("price", ascending=False)

                tasks = [
                    self._orderDataFrame(asks, 'price', ['price','asks_qty']),
                    self._orderDataFrame(bids, 'price', ['price','bids_qty'])
                ]       

                asks, bids = await asyncio.gather(*tasks)

                return last_updated_id, asks, bids
            else:
                raise ValueError("No se pudo obtener el snapshot")
        except ValueError as ve:
            print(ve)

    def __str__(self) -> str:
        return f"Symbol: {self._symbol}\nprice: ${self._price}\nVol_24_Hr: {self._vol24Hr}\n"\
            f"asks:\n{self._asks}"\
            f"\nbids:\n{self._bids}\nDatos para Actualizar:\n    "\
            f"newAsks:\n{self._newAsks}\n    newBids:\n{self._newBids}"

    # ---------------------------------------------------------------------- ATRIBUTES
    @property
    def symbol(self):
        return self._symbol

    @symbol.setter
    def symbol(self, symbol: str):
        self._symbol = symbol.upper()

    @property
    def price(self):
        return self._price

    @price.setter
    def price(self, price: float):
        self._price = price

    @property
    def vol24Hr(self):
        return self._vol24Hr

    @vol24Hr.setter
    def vol24Hr(self, vol24Hr: float):
        self._vol24Hr = vol24Hr

    @property
    def newAsks(self):
        return self._newAsks

    @newAsks.setter
    def newAsks(self, newAsks: pd.DataFrame):
        self._newAsks = newAsks

    @property
    def newBids(self):
        return self._newBids

    @newBids.setter
    def newBids(self, newBids: pd.DataFrame):
        self._newBids = newBids

    @property
    def asks(self):
        return self._asks

    @asks.setter
    def asks(self, asks: pd.DataFrame):
        self._asks = asks

    @property
    def bids(self):
        return self._bids

    @bids.setter
    def bids(self, bids: pd.DataFrame):
        self._bids = bids
    
    @property
    def sp_v_m1(self):
        return self._sp_v_m1
    
    @property
    def sp_v_m2(self):
        return self._sp_v_m2
    
    @property
    def sp_c_m1(self):
        return self._sp_c_m1
    
    @property
    def sp_c_m2(self):
        return self._sp_c_m2