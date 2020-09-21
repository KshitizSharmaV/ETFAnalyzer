import sys

sys.path.append('..')
from functools import partial
from datetime import datetime, timedelta
from CalculateETFArbitrage.Helpers.LoadEtfHoldings import LoadHoldingsdata
from PolygonTickData.PolygonCreateURLS import PolgonDataCreateURLS
from MongoDB.MongoDBConnections import MongoDBConnectors
from CalculateETFArbitrage.TradesQuotesRunner import TradesQuotesProcesses


class FetchAndSaveHistoricalPerSecData():
    def __init__(self, date=None, etf_name=None):
        self.connection = MongoDBConnectors().get_pymongo_devlocal_devlocal()
        self.per_sec_trades_db = self.connection.ETF_db.PerSecLiveTrades
        self.per_sec_quotes_db = self.connection.ETF_db.PerSecLiveQuotes
        self.date = date
        self.etf_name = etf_name
        self.etf_data = LoadHoldingsdata().LoadHoldingsAndClean(etfname=etf_name, fundholdingsdate=date)

    def create_urls_for_trades(self, symbols=None, date=None, endTs=None):
        """Create API URLs for Trades data for ETF"""
        routines = map(partial(PolgonDataCreateURLS().PolygonHistoricTrades, date=date, startTS=None, endTS=endTs,
                               limitresult=str(50000)), symbols)
        symbol_status = {symbol: {'batchSize': 0} for symbol in symbols}
        routines = list(routines)
        return routines, symbol_status

    """Using existing historic quotes create url method"""

    def data_fetch_and_store_runner(self, trades_quotes_proc_obj, collection_name=None):
        if collection_name == self.per_sec_trades_db:
            trades_quotes_proc_obj.fetch_and_store_runner(collection_name=collection_name,
                                                          trades_per_sec_create_url_func=self.create_urls_for_trades)
        else:
            trades_quotes_proc_obj.fetch_and_store_runner(collection_name=collection_name)

    def all_process_runner(self):
        print("HISTORICAL PER SEC DATA PROCESS:...")
        self.data_fetch_and_store_runner(
            trades_quotes_proc_obj=TradesQuotesProcesses(symbols=self.etf_data.getSymbols(), date=self.date),
            collection_name=self.per_sec_trades_db)
        print("HISTORICAL PER SEC DATA PROCESS:...")
        self.data_fetch_and_store_runner(
            trades_quotes_proc_obj=TradesQuotesProcesses(symbols=[self.etf_name], date=self.date),
            collection_name=self.per_sec_quotes_db)


if __name__ == '__main__':
    FetchAndSaveHistoricalPerSecData(
        date=(datetime.now() - timedelta(days=3)).strftime("%Y-%m-%d"), etf_name='VO').all_process_runner()
