#
import sys

sys.path.append("..")
#
import datetime
import pandas as pd
from AlpacaTrading.trade_funcs import *
from MongoDB.PerMinDataOperations import PerMinDataOperations
from CommonServices.MultiProcessingTasks import CPUBonundThreading


def set_position(_position_dict, position_type="", position_size=0,
                 position_open_time=datetime.datetime.now(), position_open_price=0, position_open_arbitrage=0,
                 position_open_abs_arbitrage=0):
    _position_dict['position_type'] = position_type
    _position_dict['position_size'] = position_size
    _position_dict['position_open_time'] = position_open_time
    _position_dict['position_open_price'] = position_open_price
    _position_dict['position_open_arbitrage'] = position_open_arbitrage
    _position_dict['position_open_abs_arbitrage'] = position_open_abs_arbitrage
    _position_dict['position_status'] = 'open'


def unset_position(_position_dict, position_close_time=datetime.datetime.now(),
                   position_close_price=0, position_close_arbitrage=0, position_close_abs_arbitrage=0):
    _position_dict['position_close_time'] = position_close_time
    _position_dict['position_close_price'] = position_close_price
    _position_dict['position_close_arbitrage'] = position_close_arbitrage
    _position_dict['position_close_abs_arbitrage'] = position_close_abs_arbitrage
    _position_dict['position_status'] = 'close'
    _position_dict['last_order_id'] = ''


def check_order_filled(order_id):
    try:
        if not order_id:
            return False
        order = get_order_by_order_id(order_id)
        if order['_raw']['filled_at']:
            return True
        else:
            return False
    except Exception as e:
        print(e)
        return False


def cancel_unfilled_orders(_position_dict):
    order_id = _position_dict['last_order_id']
    check = check_order_filled(order_id)
    if not check:
        cancel_order_by_id(order_id)
        _position_dict['position_status'] = 'cancelled'
    return _position_dict


def cancel_all_unfilled_orders_threaded(_position_dicts_all_etf: dict):
    return list(CPUBonundThreading(cancel_unfilled_orders, list(_position_dicts_all_etf.values())))


def cancel_all_unfilled_orders(_position_dicts_all_etf: dict):
    cancel_all_open_orders()
    for k, v in _position_dicts_all_etf.items():
        if v['position_status'] == 'open':
            v['position_status'] = 'cancelled'
            v['last_order_id'] = ''


def get_position_status(_position_dict):
    """Determine Position Status -- Open or Close"""
    if _position_dict['position_status'] == 'open':
        return True
    else:
        return False


def fetch_arbitrage(etf_name='VO') -> pd.DataFrame:
    record_df = PerMinDataOperations().LiveFetchPerMinArbitrage(etfname=etf_name)
    return record_df


if __name__ == '__main__':
    check_order_filled('eb546a91-93f7-4b6f-bc09-8937af86a354')
