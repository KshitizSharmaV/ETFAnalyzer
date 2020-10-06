import socket
import sys

sys.path.append('..')

from itertools import chain
import time
from datetime import datetime, timedelta
from CalculateETFArbitrage.Helpers.LoadEtfHoldings import LoadHoldingsdata
from PerSecLive.CalcPer10Sec import main_runner
from PerSecLive.DataFetch import FetchAndSaveHistoricalPerSecData, runner_for_etf
from CommonServices.LogCreater import CreateLogger
from CommonServices.MultiProcessingTasks import CPUBonundThreading
from MongoDB.MongoDBConnections import MongoDBConnectors

logObj = CreateLogger()
logger = logObj.createLogFile(dirName="PerSecLive/", logFileName="-PerSecLiveCaller.log",
                              loggerName="PerSecLiveCallerLog")


def collections_dropper():
    sys_private_ip = socket.gethostbyname(socket.gethostname())
    if sys_private_ip == '172.31.76.32':
        connection = MongoDBConnectors().get_pymongo_readWrite_production_production()
    else:
        connection = MongoDBConnectors().get_pymongo_devlocal_devlocal()
    per_sec_trades_db = connection.ETF_db.PerSecLiveTrades
    per_sec_quotes_db = connection.ETF_db.PerSecLiveQuotes
    connection.ETF_db.drop_collection(per_sec_trades_db)
    connection.ETF_db.drop_collection(per_sec_quotes_db)


def get_unique_ticker_list_for_trades(top_10_etf_list):
    load_holdings_object = LoadHoldingsdata()
    unique_ticker_list = [
        load_holdings_object.LoadHoldingsAndClean(etfname=etf, fundholdingsdate=_date).getSymbols()
        for etf in top_10_etf_list]
    unique_ticker_list = list(chain.from_iterable(unique_ticker_list))
    unique_ticker_list = [ticker.split(' ')[0] for ticker in unique_ticker_list]
    unique_ticker_list = list(set(unique_ticker_list))
    return unique_ticker_list


def runner(_date):
    try:
        """Date for PerSecLive Operation -- (MODIFY BEFORE USE) in string format"""
        # _date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

        """ETF List (MODIFIABLE)"""
        top_10_etf_list = ['SPY', 'IVV', 'VOO', 'QQQ', 'VUG', 'VTV', 'IJH', 'IJR', 'VGT', 'VO']

        """Get Unique tickers from all ETFs"""
        unique_ticker_list = get_unique_ticker_list_for_trades(top_10_etf_list)

        """Data Fetch from Polygon API"""
        checkpoint = time.time()
        FetchAndSaveHistoricalPerSecData().all_process_runner_trades(symbols=unique_ticker_list, date_for=_date)
        quotes_func_cover = lambda x: runner_for_etf(etf_name=x, date_for=_date)
        CPUBonundThreading(quotes_func_cover, top_10_etf_list)
        logger.debug(f"Total data fetch time : {time.time() - checkpoint} seconds")
        print(f"Total data fetch time : {time.time() - checkpoint} seconds")

        """Arbitrage Calculation and Store in DB"""
        checkpoint = time.time()
        main_runner(etf_list=top_10_etf_list, _date=datetime.strptime(_date, '%Y-%m-%d'),
                    ticker_list=unique_ticker_list)
        logger.debug(f"Total calculation time : {time.time() - checkpoint} seconds")
        print(f"Total calculation time : {time.time() - checkpoint} seconds")

        """Drop PerSecLiveTrades and PerSecLiveQuotes collection for the day after arbitrage calculation done."""
        collections_dropper()
        logger.debug(f"Trades/Quotes Collections dropped for {_date}")
        print(f"Trades/Quotes Collections dropped for {_date}")
    except Exception as e:
        logger.exception(e)
        print(e)
        pass


if __name__ == "__main__":
    date_list = ['2020-08-31']
    for _date in date_list:
        runner(_date=_date)
