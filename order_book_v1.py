from binance.client import Client
from coin_data import data
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import math


# ----------------------------------

"""
Summary: 
    Función que permite obtener el precio actual de la moneda
Params:
    - ticker: str --> tycker symbol de la moenda
Returns:
    - (float) --> El precio de la moneda
"""


def get_price(ticker: str) -> float:
    return float(Client().futures_mark_price(symbol=ticker)["markPrice"])


"""
Summary:
    Función que permie obtener el libro de órdenes
Params:
    - ticker: str --> ticker symbol de la moneda
Returns:
    - (dict) --> Retorna un diccionario con los datos del libro órdenes
"""


def get_order_book(ticker: str):
    # return Client().futures_order_book(symbol=ticker)
    return Client().futures_order_book(symbol=ticker, limit=1000)


"""
Summary:
    Función para solicitar al usuario la moneda
Returns:
    - (str) --> Un texto con la moneda en mayúsculas
"""


def ask_for_ticker():
    # FALTA EL MANEJO DE LOS POSIBLES ERRORES
    # considerar: ingreso de datos vacíos, que
    t = input("Ingresa el ticker (sin el USDT)-> ")
    return t.upper()


def buscar_moneda_en_exchange(ticker: str) -> bool:
    pass


"""
Summary:
    Función para obtener solo los precios de venta y compras junto con las monedas
Params:
    - order_book: dict --> Recibe el libro de órdenes obtenido anteriormente, el cual es un diccionario
Returns:
    - (pd.DataFrame) --> Retorna un Pandas DataFrame de las compras y ventas
"""


def get_compras_ventas(order_book: dict) -> pd.DataFrame:
    ventas = pd.DataFrame(order_book["asks"], columns=["precio", "monedas"]).sort_index(
        ascending=False
    )
    ventas[["precio", "monedas"]] = ventas[["precio", "monedas"]].astype(float)
    compras = pd.DataFrame(order_book["bids"], columns=["precio", "monedas"])
    compras[["precio", "monedas"]] = compras[["precio", "monedas"]].astype(float)
    return ventas, compras


def get_rango_ventas(min: float, max: float, inc: float) -> np.arange:
    rango_v_i = math.floor(min / inc) * inc
    rango_v_f = math.ceil(max / inc) * inc
    return np.arange(rango_v_i, rango_v_f, inc)


def get_rango_compras(min: float, max: float, inc: float) -> np.arange:
    rango_c_i = math.floor(min / inc) * inc
    rango_c_f = (math.ceil(max / inc) * inc)
    return np.arange(rango_c_i, rango_c_f, inc)


def get_shock_points(ventas, compras, incremento):
    ventas_max = ventas["precio"].max()
    ventas_min = ventas["precio"].min()
    compras_max = compras["precio"].max()
    compras_min = compras["precio"].min()
    rango_v = get_rango_ventas(ventas_min, ventas_max, incremento)
    rango_c = get_rango_ventas(compras_min, compras_max, incremento)
    
    # Agrupdamos las ventas y compras por rangos
    compras_ag = compras.groupby(pd.cut(compras.precio, rango_c)).sum()
    ventas_ag = ventas.groupby(pd.cut(ventas.precio, rango_v)).sum()
    # Obtenemos el indice de la máxima cantidad
    idx_c = compras_ag.monedas.idxmax()
    idx_v = ventas_ag.monedas.idxmax()
    # Obtenemos el valor correspondiente al precio
    precio_compra = idx_c.right
    precio_venta = idx_v.right
    # Monedas 
    monedas_compra = compras_ag["monedas"].max()
    monedas_venta = ventas_ag["monedas"].max()
    #print(f'rango compras{precio_compra}, {monedas_compra}')
    #print(f'rango ventas{precio_venta}, {monedas_venta}')
    print(compras_ag)
    return precio_compra, monedas_compra, precio_venta, monedas_venta


def get_all_shock_points(ticker, ventas, compras) -> pd.DataFrame:
    shocks_compra = {
        'precio':[],
        'monedas': []
    }
    shocks_venta = {
        'precio':[],
        'monedas': []
    }
    for i in range(1, len(data[ticker])):
        inc = data[ticker]["i" + str(i)]
        print(inc)
        p_c, m_c, p_v, m_v = get_shock_points(ventas, compras, inc)
        shocks_compra['precio'].append(p_c)
        shocks_compra['monedas'].append(m_c)
        shocks_venta['precio'].append(p_v)
        shocks_venta['monedas'].append(m_v)
    return pd.DataFrame(shocks_venta), pd.DataFrame(shocks_compra)


def run(ticker: str, precio_actual: float, order_book: dict):
    ventas, compras = get_compras_ventas(order_book)
    order_book = pd.DataFrame.from_dict(order_book, orient="index")
    sp_v, sp_c = get_all_shock_points(ticker, ventas, compras)
    print(sp_v.sort_values('precio'))
    print(sp_c.sort_values('precio', ascending=False))


if __name__ == "__main__":
    ticker = "ADAUSDT"
    # ticker = ask_for_ticker()
    precio_actual = get_price(ticker)
    order_book = get_order_book(ticker)
    run(ticker, precio_actual, order_book)
