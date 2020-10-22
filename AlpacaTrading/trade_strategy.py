#
import sys

sys.path.append("..")
#
import time
import schedule
import datetime
import pprint
import pandas as pd
from typing import TypedDict
from AlpacaTrading.trade_funcs import *
from AlpacaTrading.Helpers import *
from MongoDB.PerMinDataOperations import PerMinDataOperations
from CommonServices.MultiProcessingTasks import multi_processing_method
from functools import partial


class Position(TypedDict):
    position_status: str
    position_for: str
    position_type: str
    position_size: int
    position_open_time: datetime.datetime
    position_open_price: float
    position_open_arbitrage: float
    position_open_abs_arbitrage: float
    position_close_time: datetime.datetime
    position_close_price: float
    position_close_arbitrage: float
    position_close_abs_arbitrage: float
    last_order_id: str


position_dicts = {}


def open_position(_position_dict, position_for='VO', position_type="", position_size=0,
                  position_open_time=datetime.datetime.now(), position_open_price=0, position_open_arbitrage=0,
                  position_open_abs_arbitrage=0):
    """Open position"""
    order = make_order(symbol=position_for, qty=position_size, side='buy', _type='limit',
                       limit_price=position_open_price)
    _position_dict['last_order_id'] = order.id
    set_position(_position_dict, position_type, position_size,
                 position_open_time, position_open_price, position_open_arbitrage,
                 position_open_abs_arbitrage)
    return _position_dict


def close_position(_position_dict, position_for='VO', position_close_time=datetime.datetime.now(),
                   position_close_price=0, position_close_arbitrage=0, position_close_abs_arbitrage=0):
    """Close an open position"""
    make_order(symbol=position_for, qty=_position_dict['position_size'], side='sell')
    unset_position(_position_dict, position_close_time, position_close_price, position_close_arbitrage,
                   position_close_abs_arbitrage)
    return _position_dict


def strategy_for_position(position_for='VO'):
    """Decide when to open/close a Long position"""
    # print("Current Position status : ")
    # pprint.pprint(position_dicts[position_for], sort_dicts=False)

    arbitrage_rec = fetch_arbitrage(etf_name=position_for)
    # print(f"Arbitrage Record: \n{arbitrage_rec}")
    arbitrage = arbitrage_rec['Arbitrage in $'][0]
    spread = arbitrage_rec['ETF Trading Spread in $'][0]
    abs_arbitrage = abs(arbitrage) - spread if (arbitrage < 0 and abs(arbitrage) > spread) else 0
    # print(f"Abs_Arbitrage: {abs_arbitrage}")

    if get_position_status(position_dicts[position_for]):
        close_position(position_dicts[position_for], position_for=position_for,
                       position_close_time=datetime.datetime.now().replace(second=0, microsecond=0),
                       position_close_price=arbitrage_rec['ETF Price'][0],
                       position_close_arbitrage=arbitrage_rec['Arbitrage in $'][0],
                       position_close_abs_arbitrage=abs_arbitrage)
        print("#*#*#*#*#* POSITION CLOSED #*#*#*#*#*")
        return

    if abs_arbitrage > 0.10:
        open_position(position_dicts[position_for], position_for=position_for, position_type="long", position_size=5,
                      position_open_time=datetime.datetime.now().replace(second=0, microsecond=0),
                      position_open_price=arbitrage_rec['ETF Price'][0] + 0.02,
                      position_open_arbitrage=arbitrage_rec['Arbitrage in $'][0],
                      position_open_abs_arbitrage=abs_arbitrage)
        print("#*#*#*#*#* POSITION OPENED #*#*#*#*#*")

    # print("Positions after strategy execution:")
    # pprint.pprint(position_dicts[position_for], sort_dicts=False)
    print(f"Cycle for {position_for} : {position_dicts[position_for]['last_order_id']}")
    # print("########################################################################################################")


def open_or_close_position_threaded(etf_list):
    # schedule.every().minute.at(":08").do(strategy_for_position, etf)
    # schedule.every().minute.at(":59").do(cancel_unfilled_orders(_position_dicts[etf]))
    # while True:
    #     schedule.run_pending()
    #     time.sleep(1)
    return list(CPUBonundThreading(strategy_for_position, etf_list))


def caller(etf_list):
    """scheduled every minute, Close the existing position if any, feed new arb data to strategy"""
    for etf in etf_list:
        position_dicts.update({etf: Position(position_for=etf, position_type="", position_size=0,
                                             position_open_time=datetime.datetime.now(),
                                             position_open_price=0, position_open_arbitrage=0,
                                             position_open_abs_arbitrage=0,
                                             position_close_time=datetime.datetime.now(), position_close_price=0,
                                             position_close_arbitrage=0, position_close_abs_arbitrage=0,
                                             position_status="", last_order_id="")})

    # part_func = partial(open_or_close_position_threaded, position_dicts)
    # list(multi_processing_method(part_func, etf_list, max_workers=7))
    schedule.every().minute.at(":08").do(open_or_close_position_threaded, etf_list)
    # schedule.every().minute.at(":59").do(cancel_all_unfilled_orders_threaded, position_dicts)
    schedule.every().minute.at(":59").do(cancel_all_unfilled_orders, position_dicts)
    while True:
        schedule.run_pending()
        time.sleep(1)


if __name__ == '__main__':
    caller(['VO', 'FTEC', 'VV', 'SCHX', 'SCHG', 'MGK', 'IWS'])