from ast import For
import asyncio
import json
import pandas as pd
import numpy as np
import os

from colorama import init, Fore, Style
from order_book import OrderBook
from miniTicker_stream import main as mmT
from depth_stream import main as mds
from coin_data import data as d


order_book = None
signal_is_sent = False
sell_entrance = 0.0
buy_entrance = 0.0
init()


async def analyze_sell_points(symbol: str, sp_v_m1: float, sp_v_m2: float, sp_c_m1: float, sp_c_m2: float, sl: float) -> dict:

    nearest = np.max([sp_c_m1, sp_c_m2])
    if (sp_v_m1 > sp_v_m2):
        pct_ventas = round((abs(sp_v_m2 - sp_v_m1)/sp_v_m2)*100, 2)
        pct_sep_1 = round((abs(sp_v_m2 - nearest)/sp_v_m2)*100, 2)
        pct_sep_2 = round((abs(nearest - sp_v_m2)/nearest)*100, 2)
        pct_sep = round((pct_sep_1+pct_sep_2)/2, 2)

        if (pct_sep/(pct_ventas+sl)) >= 2:
            return {
                'type': "Sell",
                'sp_v_m1' : sp_v_m1,
                'sp_v_m2' : sp_v_m2,
                'sp_c_m2' : sp_c_m2,
                'sp_c_m1' : sp_c_m1,
                'enter_at': sp_v_m2,
                'sl':  round(pct_ventas+sl,d[symbol]['price_decimal']),
                'tp': nearest
            }

    return None


async def analyze_buy_points(symbol: str, sp_v_m1: float, sp_v_m2: float, sp_c_m1: float, sp_c_m2: float, sl: float) -> dict:

    nearest = np.min([sp_v_m1, sp_v_m2])

    if (sp_c_m2 > sp_c_m1) and (sp_c_m1 > 0):
        pct_compras = round((abs(sp_c_m2 - sp_c_m1)/sp_c_m2)*100, 2)
        pct_sep_1 = round((abs(sp_c_m2 - nearest)/sp_c_m2)*100, 2)
        pct_sep_2 = round((abs(nearest - sp_c_m2)/nearest)*100, 2)
        pct_sep = round((pct_sep_1+pct_sep_2)/2, 2)

        if (pct_sep/(pct_compras+sl)) >= 2:
            return {
                'type': "Buy",
                'sp_v_m1' : sp_v_m1,
                'sp_v_m2' : sp_v_m2,
                'sp_c_m2' : sp_c_m2,
                'sp_c_m1' : sp_c_m1,
                'enter_at': sp_c_m2,
                'sl':  round(sp_c_m2/pct_compras+sl, d[symbol]['price_decimal']),
                'tp': nearest
            }

    return None


async def analyze_points(symbol: str, sp_v_m1: float, sp_v_m2: float, sp_c_m1: float, sp_c_m2: float) -> bool:
    # Aquí se verifica la distancia entre los puntos de venta y la distancia entre el mínimo y el más cercano (de compras)
    # y checar que den un 1:2, considerando el SL que será por default de un 0.20% siempre que no tengamos en el
    # diccionario la llave "sl"
    flag_venta = False
    flag_compra = False
    sl = 0.2 if "sl" not in d[symbol] else d[symbol]['sl']
    # la "sp_v_m" multiplo 1 -> más alejado del precio
    # la "sp_v_M" multiplo 2 -> más cercano al precio
    # la "sp_c_m" multiplo 1 -> más alejado del precio
    # la "sp_c_M" multiplo 2 -> más cercano al precio
    if (sp_v_m1 > sp_v_m2) and (sp_c_m2 > sp_c_m1) and (sp_c_m1 > 0):
        pct_ventas = round((abs(sp_v_m2 - sp_v_m1)/sp_v_m2)*100, 2)
        pct_compras = round((abs(sp_c_m2 - sp_c_m1)/sp_c_m2)*100, 2)
        pct_sep_1 = round((abs(sp_v_m2 - sp_c_m2)/sp_v_m2)*100, 2)
        pct_sep_2 = round((abs(sp_c_m2 - sp_v_m2)/sp_c_m2)*100, 2)
        pct_sep = round((pct_sep_1+pct_sep_2)/2, 2)

    if (pct_sep/(pct_ventas+sl)) >= 2:
        flag_venta = True
    if (pct_sep/(pct_compras+sl)) >= 2:
        flag_compra = True

    return flag_venta, flag_compra


async def analyze_data(symbol: str):
    global order_book, signal_is_sent

    while True:

        # si tenemos datos, es deicir, precio y libro (asks y bids) procedemos a analizar sino no
        if order_book.price > 0 and len(order_book.asks.index) > 0 and len(order_book.bids.index) > 0:
            order_book = await order_book.get_shock_points()
            # Ya con los puntos y todos los datos (Precio y puntos de venta) tomamos decisiones

            # i. Analizamos los puntos y verificamos si tenemos una entrada

            sl = 0.2 if "sl" not in d[symbol] else d[symbol]['sl']

            tasks = [
                analyze_sell_points(symbol, order_book.sp_v_m1, order_book.sp_v_m2,
                                    order_book.sp_c_m1, order_book.sp_c_m2, sl),
                analyze_buy_points(symbol, order_book.sp_v_m1, order_book.sp_v_m2,
                                   order_book.sp_c_m1, order_book.sp_c_m2, sl)
            ]

            sell_data, buy_data = await asyncio.gather(*tasks)

            distance = 1 if "price_dist" not in d[symbol] else d[symbol]['price_dist']

            price = order_book.price
            buy_price_distance = round(
                (abs(order_book.sp_c_m2 - price)/order_book.sp_c_m2)*100, 2)
            sell_price_distance = round(
                (abs(order_book.sp_v_m2 - price)/order_book.sp_v_m2)*100, 2)

            
            # Aquí signal_is_sent = False en vuelta 1
            if sell_data is not None and buy_data is not None:

                if ((sell_price_distance <= distance) or (buy_price_distance <= distance)):                   
                    signal_is_sent = True
                    print(f"{'*'*50}")
                    print(
                        f"Señal de {Fore.RED}VENTA{Style.RESET_ALL} y {Fore.GREEN}COMPRA{Style.RESET_ALL} en {Fore.YELLOW}{symbol}{Style.RESET_ALL}")
                    print(f"{'*' * 50}")
                    print(f"SL considerado: {sl}%")
                    print("Puntos de Venta")
                    print(f"2.- ${sell_data['sp_v_m1']}")
                    print(f"1.- ${sell_data['sp_v_m2']}")
                    print("")
                    print("Puntos de Compra")
                    print(f"1.- ${buy_data['sp_v_m2']}")
                    print(f"2.- ${buy_data['sp_v_m1']}")
                    print("")
                while sell_price_distance <= distance or buy_price_distance<= distance:
                    #print("Manejando posición")
                    continue
            else:
                signal_is_sent = False

                # Si solo se cumple la venta, calculamos la distancia entre precio de entrada de venta y el precio
            if sell_data is not None and buy_data is None:
                #print(sell_price_distance,"%")
                if (sell_price_distance <= distance):                    
                    signal_is_sent = True
                    print(f"{'*'*50}")
                    print(
                        f"Señal de {Fore.RED}VENTA{Style.RESET_ALL} en {Fore.YELLOW}{symbol}{Style.RESET_ALL}")
                    print(f"{'*' * 50}")
                    print(f"SL considerado: {sl}%")
                    print("Puntos de Venta")
                    print(f"2.- ${sell_data['sp_v_m1']}")
                    print(f"1.- ${sell_data['sp_v_m2']}")
                    print("")
                    print("Puntos de Compra")
                    print(f"1.- ${buy_data['sp_v_m2']}")
                    print(f"2.- ${buy_data['sp_v_m1']}")
                    print("")
                while sell_price_distance <= distance:
                    #print("Manejando posición")
                    continue

            else:
                signal_is_sent = False

            if sell_data is None and buy_data is not None:                
                #print(buy_price_distance,"%")
                if (buy_price_distance <= distance):
                    signal_is_sent = True
                    print(f"{'*'*50}")
                    print(
                        f"Señal de {Fore.GREEN}COMPRA{Style.RESET_ALL} en {Fore.YELLOW}{symbol}{Style.RESET_ALL}")
                    print(f"{'*' * 50}")
                    print(f"SL considerado: {sl}%")
                    print(f"2.- ${sell_data['sp_v_m1']}")
                    print(f"1.- ${sell_data['sp_v_m2']}")
                    print("")
                    print("Puntos de Compra")
                    print(f"1.- ${buy_data['sp_v_m2']}")
                    print(f"2.- ${buy_data['sp_v_m1']}")
                    print("")
                while buy_price_distance <= distance:
                    #print("Manejando posición")
                    continue

            else:
                signal_is_sent = False

        await asyncio.sleep(0.1)


async def main(symbol: str):
    global order_book
    order_book = OrderBook(symbol)

    tasks = [mmT(symbol, order_book), mds(
        symbol, order_book), analyze_data(symbol)]
    await asyncio.gather(*tasks)


if __name__ == '__main__':
    asyncio.run(main("MANAUSDT"))
