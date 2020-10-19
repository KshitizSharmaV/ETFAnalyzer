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
from MongoDB.PerMinDataOperations import PerMinDataOperations


class Position(TypedDict):
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
    position_status: str


position_dict = Position(position_type="", position_size=0, position_open_time=datetime.datetime.now(),
                         position_open_price=0, position_open_arbitrage=0, position_open_abs_arbitrage=0,
                         position_close_time=datetime.datetime.now(), position_close_price=0,
                         position_close_arbitrage=0, position_close_abs_arbitrage=0, position_status="")


def get_position_status(_position_dict):
    """Determine Position Status -- Open or Close"""
    if _position_dict['position_status'] == 'Open':
        return True
    else:
        return False


def open_position(_position_dict, position_for='VO', position_type="", position_size=0,
                  position_open_time=datetime.datetime.now(), position_open_price=0, position_open_arbitrage=0,
                  position_open_abs_arbitrage=0):
    """Open position"""
    if not get_position_status(_position_dict):
        _position_dict['position_type'] = position_type
        _position_dict['position_size'] = position_size
        _position_dict['position_open_time'] = position_open_time
        _position_dict['position_open_price'] = position_open_price
        _position_dict['position_open_arbitrage'] = position_open_arbitrage
        _position_dict['position_open_abs_arbitrage'] = position_open_abs_arbitrage
        _position_dict['position_status'] = 'open'
        make_order(symbol=position_for, qty=_position_dict['position_size'], side='buy')
    return _position_dict


def close_position(_position_dict, position_for='VO', position_close_time=datetime.datetime.now(),
                   position_close_price=0, position_close_arbitrage=0, position_close_abs_arbitrage=0):
    """Close an open position"""
    if get_position_status(_position_dict):
        _position_dict['position_close_time'] = position_close_time
        _position_dict['position_close_price'] = position_close_price
        _position_dict['position_close_arbitrage'] = position_close_arbitrage
        _position_dict['position_close_abs_arbitrage'] = position_close_abs_arbitrage
        _position_dict['position_status'] = 'close'
        make_order(symbol=position_for, qty=_position_dict['position_size'], side='sell')
    return _position_dict


def fetch_arbitrage(etf_name='VO') -> pd.DataFrame:
    record_df = PerMinDataOperations().LiveFetchPerMinArbitrage(etfname=etf_name)
    return record_df


def strategy_for_position(position_for='VO'):
    """Decide when to open/close a Long position"""
    print("Current Position status : ")
    pprint.pprint(position_dict)
    arbitrage_rec = fetch_arbitrage(etf_name=position_for)
    print(f"Arbitrage Record: \n{arbitrage_rec}")
    arbitrage = arbitrage_rec['Arbitrage in $'][0]
    spread = arbitrage_rec['ETF Trading Spread in $'][0]
    abs_arbitrage = abs(arbitrage) - spread if (arbitrage < 0 and abs(arbitrage) > spread) else 0
    print(f"Abs_Arbitrage: {abs_arbitrage}")
    if get_position_status(position_dict):
        close_position(position_dict, position_for=position_for,
                       position_close_time=datetime.datetime.now().replace(second=0, microsecond=0),
                       position_close_price=arbitrage_rec['ETF Price'][0],
                       position_close_arbitrage=arbitrage_rec['Arbitrage in $'][0],
                       position_close_abs_arbitrage=abs_arbitrage)
        return
    if abs_arbitrage > 0.05:
        open_position(position_dict, position_for=position_for, position_type="long", position_size=5,
                      position_open_time=datetime.datetime.now().replace(second=0, microsecond=0),
                      position_open_price=arbitrage_rec['ETF Price'][0],
                      position_open_arbitrage=arbitrage_rec['Arbitrage in $'][0],
                      position_open_abs_arbitrage=abs_arbitrage)
    print("Positions after strategy execution:")
    pprint.pprint(position_dict)
    print("########################################################################################################")


def caller():
    """scheduled every minute, Close the existing position if any, feed new arb data to strategy"""
    schedule.every().minute.at(":08").do(strategy_for_position, 'VO')
    while True:
        schedule.run_pending()
        time.sleep(1)


if __name__ == '__main__':
    caller()
