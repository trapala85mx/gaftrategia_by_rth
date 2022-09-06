from dataclasses import dataclass
from pandas import DataFrame


@dataclass
class OrderBook:
    symbol: str
    price: float = 0.0
    asks: DataFrame = None
    bids: DataFrame = None
    sp_v_m1: float = 0.0
    sp_v_m2: float = 0.0
    sp_c_m1: float = 0.0
    sp_c_m2: float = 0.0
    u_anterior: int = 0
    pu_actual: int = 0
    U: int = 0
    u: int = 0
    last_updated_id: int = 0