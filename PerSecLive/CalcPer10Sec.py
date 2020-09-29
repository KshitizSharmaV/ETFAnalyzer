import sys

sys.path.append("../")

import time
import traceback
from functools import partial
from itertools import chain
from CommonServices.MultiProcessingTasks import multi_processing_method, CPUBonundThreading
from PerSecLive.Helpers import *


def main_runner(etf_list, _date):
    ####################################################################################################################
    # CONSTANT TIME BLOCK
    ####################################################################################################################
    """Ticker list with ETF names included"""
    ticker_list = list(map(partial(get_ticker_list_for_etf, _date), etf_list))
    ticker_list = list(set(list(chain.from_iterable(ticker_list))))
    """Day Start and End time"""
    start, end = get_local_time_for_date(date_for=_date)
    """Per second Timestamps from Day Start to Day End"""
    ts_ranges = get_timestamp_ranges_1sec(start=start, end=end)
    """Trades Dict initialization"""
    trades_dict = {ticker: TradeStruct(symbol=ticker, priceT=0) for ticker in ticker_list}
    """Make ETF objects for all ETFs"""
    etf_object_list = list(map(partial(make_etf_objects, _date), etf_list))
    ####################################################################################################################
    all_arb = []
    for idx in range(1, len(ts_ranges)):
        checkpoint6 = time.time()
        start_ts = ts_ranges[idx - 1]
        end_ts = ts_ranges[idx]
        """ Every second update trades_dict with current prices """
        fetch_timer1 = time.time()
        trade_dict_operation(ticker_list=ticker_list, start_ts=start_ts, end_ts=end_ts, trades_dict=trades_dict)
        fetch_timer2 = time.time()
        calc_maintainer_partial_func = partial(calculation_maintainer, trades_dict, start_ts, end_ts)
        # futures = multi_processing_method(calc_maintainer_partial_func, etf_object_list, max_workers=None)
        futures = CPUBonundThreading(calc_maintainer_partial_func, etf_object_list)
        checkpoint8 = time.time()
        all_arb.append(list(futures))
        checkpoint7 = time.time()
        print(f"Data Fetch time: {fetch_timer2 - fetch_timer1} seconds")
        print(f"Multiprocessing time: {checkpoint8 - fetch_timer2}")
        print(f"Collating all futures time: {checkpoint7 - checkpoint8}")
        print(f"Data Fetch + Calculation time: {checkpoint7 - checkpoint6} seconds")
    return all_arb


def calculation_maintainer(trades_dict, start_ts, end_ts, etf_object):
    holdings_dict, etf_name = etf_object.holdings_dict, etf_object.etf_name
    ticker_list_without_etf = filter(lambda x: x != 'CASH', list(holdings_dict.keys()))
    arbitrage = calculate_arbitrage_for_etf_and_date(etf_name=etf_name,
                                                     tick_list=ticker_list_without_etf,
                                                     start_ts=start_ts,
                                                     end_ts=end_ts,
                                                     holdings_dict=holdings_dict,
                                                     trades_dict=trades_dict)
    return arbitrage


def calculate_arbitrage_for_etf_and_date(etf_name, tick_list, start_ts, end_ts, holdings_dict, trades_dict):
    try:
        checkpoint1 = time.time()
        mapper = map(lambda x: trades_dict[x].price_pct_chg * (holdings_dict[x] / 100), tick_list)
        nav = sum(list(mapper))

        etf_price = trades_dict[etf_name].priceT
        arbitrage = ((trades_dict[etf_name].price_pct_chg - nav) * etf_price) / 100

        spreadforsec = calculate_spread(etf_name=etf_name, start_ts=int(start_ts), end_ts=int(end_ts))

        print(
            f"{start_ts} - {end_ts} : Arbitrage for {etf_name} : {round(arbitrage, 8)} || ETF Price : {etf_price} || "
            f"Spread : {spreadforsec}")
        checkpoint2 = time.time()
        print(f"calculation time: {checkpoint2 - checkpoint1}seconds")

        return {'End Time': helper_object.getHumanTime(end_ts), 'ETFName': etf_name, 'Arbitrage': arbitrage,
                'ETFPrice': etf_price, 'Spread': spreadforsec}
    except Exception as e:
        traceback.print_exc()
        pass


if __name__ == '__main__':
    date_ = (datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=11))
    checkpoint4 = time.time()
    # arb = calculation_maintainer('VOO', date_)
    all_arb_final = main_runner(['VOO', 'SPY'], date_)
    print(all_arb_final)
    checkpoint5 = time.time()
    print(f"Total Time taken for all processes for ETF is: {checkpoint5 - checkpoint4} seconds")
