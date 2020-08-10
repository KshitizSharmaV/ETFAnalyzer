import sys

sys.path.append("..")  # Remove in production - KTZ

import pandas as pd
import time
import asyncio
from CalculateETFArbitrage.TradesQuotesFetchSave import FetchPolygonData
from PolygonTickData.Helper import Helper
from MongoDB.SaveFetchQuotesData import MongoTradesQuotesData
from PolygonTickData.PolygonCreateURLS import PolgonDataCreateURLS
from functools import partial


class TradesQuotesProcesses(object):
    def __init__(self, symbols=None, date=None):
        self.date = date
        self.endTs = Helper().convertHumanTimeToUnixTimeStamp(date=self.date, time='17:00:00')
        self.symbols = symbols
        self.mtqd = MongoTradesQuotesData()

    def check_if_data_exists_in_mongo_db(self, symbols=None, date=None, CollectionName=None):
        """Check if symbol,date pair exist in MongoDB, If don't exist download URLs for the symbols"""
        check_func = partial(self.mtqd.does_item_exist_in_quotes_trades_mongo_db, date=date,
                             CollectionName=CollectionName)
        symbols_to_be_downloaded = [symbol for symbol in symbols if not check_func(symbol)]
        return symbols_to_be_downloaded

    def create_urls_for_quotes(self, symbols=None, date=None, endTs=None):
        """Create API URLs for Quotes data for ETF"""
        routines = map(partial(PolgonDataCreateURLS().PolygonHistoricQuotes, date=date, startTS=None, endTS=endTs,
                               limitresult=str(50000)), symbols)
        symbol_status = {symbol: {'batchSize': 0} for symbol in symbols}
        return list(routines), symbol_status

    def create_urls_for_trade(self, symbols=None, start_date=None):
        """Create API URLs for Trades data for ETF and it's Holdings"""
        routines = map(partial(PolgonDataCreateURLS().PolygonAggregdateData, aggregateBy='minute',
                               startDate=start_date, endDate=start_date), symbols)
        return list(routines)

    def fetch_and_store_runner(self, collection_name=None, trade_data_flag=False):
        """Combined Main for Trades and Quotes data operation for an ETF -- TradesQuotesFetchSave.py"""
        symbols_to_be_downloaded = self.check_if_data_exists_in_mongo_db(symbols=self.symbols, date=self.date,
                                                                         CollectionName=collection_name)
        # Trade Configuration
        if symbols_to_be_downloaded and trade_data_flag:
            routines = self.create_urls_for_trade(symbols=symbols_to_be_downloaded, start_date=self.date)
            fetch_polygon_data_object = FetchPolygonData(date=self.date, collection_name=collection_name)
            loop = asyncio.get_event_loop()
            future = asyncio.ensure_future(
                fetch_polygon_data_object.task_creator_and_gatherer(fetch_polygon_data_object.get_trade_data_and_save,
                                                                    routines))
            loop.run_until_complete(future)

        # Quotes Configuration
        elif symbols_to_be_downloaded:
            create_url = PolgonDataCreateURLS().PolygonHistoricQuotes
            routines, symbol_status = self.create_urls_for_quotes(symbols=symbols_to_be_downloaded, date=self.date,
                                                                  endTs=self.endTs)
            fetch_polygon_data_object = FetchPolygonData(date=self.date, polygon_method=create_url,
                                                         symbol_status=symbol_status, collection_name=collection_name)
            fetch_polygon_data_object.quotes_data_operation_runner(url=routines[0])

    def get_data(self, collection_name, pipeline):
        """Fetch Quotes/Trades Data from MongoDB -- Mongo Operation in MongoDB folder"""
        result_df = pd.DataFrame(self.mtqd.fetch_quotes_trades_data_from_mongo(symbolList=self.symbols, date=self.date,
                                                                               CollectionName=collection_name,
                                                                               pipeline=pipeline))
        return result_df
