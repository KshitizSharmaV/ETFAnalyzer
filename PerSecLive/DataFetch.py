import socket
import sys

sys.path.append('..')


from functools import partial
from CalculateETFArbitrage.Helpers.LoadEtfHoldings import LoadHoldingsdata
from PolygonTickData.PolygonCreateURLS import PolgonDataCreateURLS
from MongoDB.MongoDBConnections import MongoDBConnectors
from CalculateETFArbitrage.TradesQuotesRunner import TradesQuotesProcesses
from pymongo import ASCENDING, DESCENDING
from CommonServices.LogCreater import CreateLogger
from PerSecLive.Helpers import get_timestamp_ranges_1sec, get_local_time_for_date

logObj = CreateLogger()
logger = logObj.createLogFile(dirName="PerSecLive/", logFileName="-PerSecLiveDataFetchLog.log",
                              loggerName="PerSecLiveDataFetchLog")


class FetchAndSaveHistoricalPerSecData():
    def __init__(self, etf_name=None, date_=None):
        sys_private_ip = socket.gethostbyname(socket.gethostname())
        if sys_private_ip == '172.31.76.32':
            self.connection = MongoDBConnectors().get_pymongo_readWrite_production_production()
        else:
            self.connection = MongoDBConnectors().get_pymongo_devlocal_devlocal()
        self.per_sec_live_trades = self.connection.ETF_db.PerSecLiveTrades
        self.per_sec_live_trades.create_index([("Symbol", ASCENDING), ("t", DESCENDING)])
        self.per_sec_live_quotes = self.connection.ETF_db.PerSecLiveQuotes
        self.per_sec_live_quotes.create_index([("Symbol", ASCENDING), ("t", DESCENDING)])
        self.date = date_
        self.etf_name = etf_name

    def create_urls_for_trades(self, symbols=None, date=None, endTs=None):
        """Create API URLs for Trades data for ETF"""
        routines = map(partial(PolgonDataCreateURLS().PolygonHistoricTrades, date=date, startTS=None, endTS=endTs,
                               limitresult=str(50000)), symbols)
        symbol_status = {symbol: {'batchSize': 0} for symbol in symbols}
        routines = list(routines)
        return routines, symbol_status

    # def symbol_check_trades(self, symbol, collection):
    #     ts_range = get_timestamp_ranges_1sec(*get_local_time_for_date(datetime.strptime(self.date, '%Y-%m-%d')))
    #     res = collection.find(
    #         {'Symbol': symbol, 't': {'$gte': int(ts_range[0]), '$lte': int(ts_range[len(ts_range) - 1])}})
    #     return True if len(list(res)) > 0 else False

    def all_process_runner_trades(self, symbols=None, date_for=None):
        if not symbols:
            etf_data = LoadHoldingsdata().LoadHoldingsAndClean(etfname=self.etf_name,
                                                               fundholdingsdate=self.date)
            symbols = etf_data.getSymbols()
        # symbols_to_download = [symbol for symbol in symbols if
        #                        not self.symbol_check_trades(symbol=symbol, collection=self.per_sec_live_trades)]
        trades_quotes_proc_obj = TradesQuotesProcesses(symbols=symbols, date=date_for)
        print("Processing historic trade data")
        trades_quotes_proc_obj.trades_fetch_and_store_runner_live(
            collection_name=self.connection.ETF_db.PerSecLiveTrades,
            per_sec_create_url_func=self.create_urls_for_trades)

    def all_process_runner_quotes(self):
        print("Processing historic quotes data")
        # symbol = self.etf_name if not self.symbol_check_trades(symbol=self.etf_name,
        #                                                        collection=self.per_sec_live_quotes) else None
        trades_quotes_proc_obj = TradesQuotesProcesses(symbols=[self.etf_name], date=self.date)
        trades_quotes_proc_obj.trades_fetch_and_store_runner_live(
            collection_name=self.connection.ETF_db.PerSecLiveQuotes,
            trade_data_flag=False,
            per_sec_create_url_func=trades_quotes_proc_obj.create_urls_for_quotes)


def runner_for_etf(etf_name, date_for):
    obj = FetchAndSaveHistoricalPerSecData(
        date_=date_for, etf_name=etf_name)
    obj.all_process_runner_quotes()
