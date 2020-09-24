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
        self.date = date
        self.etf_name = etf_name

    def create_urls_for_trades(self, symbols=None, date=None, endTs=None):
        """Create API URLs for Trades data for ETF"""
        routines = map(partial(PolgonDataCreateURLS().PolygonHistoricTrades, date=date, startTS=None, endTS=endTs,
                               limitresult=str(50000)), symbols)
        symbol_status = {symbol: {'batchSize': 0} for symbol in symbols}
        routines = list(routines)
        return routines, symbol_status

    def all_process_runner_trades(self):
        etf_data = LoadHoldingsdata().LoadHoldingsAndClean(etfname=self.etf_name, fundholdingsdate=self.date)
        trades_quotes_proc_obj=TradesQuotesProcesses(symbols=etf_data.getSymbols(), date=self.date)
        print("Processing historic trade data")
        trades_quotes_proc_obj.trades_fetch_and_store_runner_live(collection_name=self.connection.ETF_db.PerSecLiveTrades,
            trades_per_sec_create_url_func=self.create_urls_for_trades)
        
    def all_process_runner_quotes(self):
        print("Processing historic quotes data")
        trades_quotes_proc_obj=TradesQuotesProcesses(symbols=[self.etf_name], date=self.date)
        trades_quotes_proc_obj.fetch_and_store_runner(collection_name=self.connection.ETF_db.PerSecLiveQuotes)
        

if __name__ == '__main__':
    obj = FetchAndSaveHistoricalPerSecData(
        date=(datetime.now() - timedelta(days=2)).strftime("%Y-%m-%d"), etf_name='VO')
    obj.all_process_runner_trades()
    obj.all_process_runner_quotes()