import asyncio
import os
from binance import AsyncClient, BinanceSocketManager
from binance.client import Client
import pandas as pd


data = {'price': 0.0}
#engine = sqlalchemy.create_engine('sqlite:///manausdtStream.db')


def create_dataframe(msg):
    #df = pd.DataFrame([msg])
    #df = df.loc[:,['s', 'E', 'p']]
    #df.columns = ['symbol', 'time', 'price']
    #df.price = df.price.astype(float)
    #df.time = pd.to_datetime(df.time, unit = 'ms')
    data = float(msg['p'])
    print(data)


async def main():
    client = await AsyncClient.create()
    bsm = BinanceSocketManager(client)
    socket = bsm.trade_socket('MANAUSDT')
    async with socket as ts:
        while True:
            #await socket.__aenter__()
            msg = await ts.recv()
            #os.system('clear')
            print(msg['p'])
            # print(msg)

if __name__=='__main__':
    asyncio.run(main())