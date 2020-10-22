import traceback

import alpaca_trade_api as tradeapi
from alpaca_trade_api.entity import Order
from alpaca_trade_api.common import URL
from AlpacaTrading.config import *

base_url = "https://paper-api.alpaca.markets"

api = tradeapi.REST(key_id=API_KEY, secret_key=SECRET_KEY, base_url=URL(base_url))


def get_account_info():
    return api.get_account().__dict__


def get_all_orders(status=None, limit=None, after=None, until=None, direction=None, nested=None):
    return api.list_orders(status=status, limit=limit, after=after, until=until, direction=direction,
                           nested=nested)


def get_order_by_order_id(order_id):
    return api.get_order(order_id=order_id).__dict__


def make_order(symbol, qty, side, _type='market', time_in_force='day', limit_price=None, stop_price=None,
               client_order_id=None, order_class=None, take_profit=None, stop_loss=None, trail_price=None,
               trail_percent=None):
    resp = api.submit_order(symbol=symbol, qty=qty, side=side, type=_type, time_in_force=time_in_force,
                            limit_price=limit_price, stop_price=stop_price, client_order_id=client_order_id,
                            order_class=order_class, take_profit=take_profit, stop_loss=stop_loss,
                            trail_price=trail_price, trail_percent=trail_percent)
    if type(resp) != Order:
        return None
    return resp


def cancel_all_open_orders():
    return api.cancel_all_orders()


def cancel_order_by_id(order_id):
    try:
        return api.cancel_order(order_id=order_id)
    except Exception as e:
        traceback.print_exc()
        return None


def get_last_trade_for_symbol(symbol):
    return api.get_last_trade(symbol)


# print(make_order('AAPL', 100, 'buy', 'market', 'day'))
# print("#####*****#####")
# print(get_all_orders(status='filled'))
# print(get_order_by_order_id(order_id='668d6b14-0fb0-4919-a5c9-fed41506ebb6'))
# print(cancel_order_by_id(order_id='668d6b14-0fb0-4919-a5c9-fed41506ebb6'))
# print(cancel_all_open_orders())
