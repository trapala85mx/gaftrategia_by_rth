import websockets
import asyncio
import json
import pandas as pd
import numpy as np
import os

from order_book import OrderBook
from websockets.exceptions import ConnectionClosedError, ConnectionClosedOK

first_event = 1
u_anterior = 0

async def main(symbol: str, order_book: OrderBook):
    global first_event, u_anterior
    url = f"wss://fstream.binance.com/ws{symbol.lower()}@depth"
    while True:
        try:
            subscribe = {
                "method": "SUBSCRIBE",
                "params": [f"{symbol.lower()}@depth"],
                "id": 1,
            }

            async with websockets.connect(url, ping_interval = 300, ping_timeout=900) as websocket:
                await websocket.send(json.dumps(subscribe))

                while True:
                    msg = await websocket.recv()
                    msg = json.loads(msg)
                    if "e" in msg:
                        # Aquí actualizamos el libro y hasta el final actualizamos el order_book
                        # 1. Siempre debemos esperar que el pu actual sea igual al u anterior
                        #    para la primera ocación siempre será falso, por lo que se puede usar el snapshot aquí
                        #    ya que es lo que se debe hacer si son diferentes y siempre entrará al menos 1 vez
                        pu_actual = msg['pu']                        
                        
                        # Aqui cumplimos que si el pu actual es diferente del u anterior obtenemos snapshot
                        # también cumplimos el punto 3 de obtener primero el snapshot
                        if pu_actual != u_anterior:
                            lastUpdatedId, order_book.asks, order_book.bids = await order_book.get_snapshot()
                            
                        # Si son iguales entonces empezamos a actualizar. El primer evento que se vaya a actualizar debe
                        # cumplir U<= lastupdated_id y u>= last updated id
                        # los siguientes eventos solo deben cumplir que u>= lastUpdatedId y el evento debe ser mayor a 1
                        else:
                            u = msg['u']
                            U = msg['U']
                            
                            order_book.newAsks = pd.DataFrame(msg['a'], columns=['price','new_asks_qty'])
                            order_book.newBids = pd.DataFrame(msg['b'], columns=['price','new_bids_qty'])

                            if U <= lastUpdatedId and u >= lastUpdatedId and first_event == 1:
                                first_event += 1                                
                                order_book = await order_book.update()                                
                                #print(order_book.bids)
                                await order_book.get_shock_points()
                                #print(order_book.bids)
                                #print(f"{'*'*30}")

                            if not u < lastUpdatedId and first_event > 1:
                                first_event += 1                                
                                order_book = await order_book.update()
                                await order_book.get_shock_points()
                        
                        #print(order_book.bids)
                        u_anterior = msg['u']
                        

                        #order_book.newAsks = pd.DataFrame(msg['a'], columns=['price', 'new_asks_qty']).sort_values('price', ascending=False)
                        #order_book.newBids = pd.DataFrame(msg['b'], columns = ['price','new_bids_qty']).sort_values('price', ascending=False)

        except ConnectionClosedError as cc:
            print(cc)
            continue

        except ConnectionClosedOK as cco:
            print(cco)
            break

        except KeyboardInterrupt as ki:
            print("Cierre por Ctrl+D")
            websocket.close()

if __name__ == '__main__':
    asyncio.run(main("BTCUSDT"))
