import sys

sys.path.append("..")  # Remove in production - KTZ

from functools import partial
import getpass
import traceback
import datetime
import requests
import json
import pandas as pd

from PolygonTickData.PolygonCreateURLS import PolgonDataCreateURLS
from MongoDB.Schemas import return_daily_open_close_pipeline
from MongoDB.SaveFetchQuotesData import MongoDailyOpenCloseData
from MongoDB.MongoDBConnections import MongoDBConnectors
from CommonServices.LogCreater import CreateLogger

logger = CreateLogger().createLogFile(dirName="HistoricalArbitrage/", logFileName="-ArbEventLog.log", loggerName="DailyOCEventLogger",
                                      filemode='a')
logger2 = CreateLogger().createLogFile(dirName="HistoricalArbitrage/", logFileName="-ArbErrorLog.log", loggerName="DailyOCErrorLogger",
                                       filemode='a')


class DailyOpenCloseData(object):

    def __init__(self, symbols=None, date=None, collection_name=None):
        self.symbols = symbols
        self.date = date
        self.collection_name = collection_name
        self.daily_open_close_object = MongoDailyOpenCloseData()

    def leave_existing_and_find_symbols_to_be_downloaded(self, symbols=None, date=None, CollectionName=None):
        """Check if symbol,date pair exist in MongoDB, If don't exist download URLs for the symbols"""
        try:
            check_func = partial(self.daily_open_close_object.does_item_exist_in_daily_open_close_mongo_db, date=date,
                                 CollectionName=CollectionName)
            symbols_to_be_downloaded = [symbol for symbol in symbols if not check_func(symbol)]
            return symbols_to_be_downloaded
        except Exception as e:
            logger.exception(e)
            logger2.exception(e)
            logger2.error(f"ETF/Tickers Name(s) : {symbols}")

    def create_urls(self, symbolsToBeDownloaded=None):
        """Create URLs for symbols to be downloaded"""
        try:
            end_date = datetime.datetime.strptime(self.date, '%Y-%m-%d') + datetime.timedelta(days=1)
            end_date = end_date.strftime('%Y-%m-%d')
            create_urls = PolgonDataCreateURLS()
            return [
                create_urls.PolygonAggregdateData(symbol=symbol, aggregateBy='day', startDate=self.date,
                                                  endDate=end_date)
                for symbol in symbolsToBeDownloaded]
        except Exception as e:
            logger.exception(e)
            logger2.exception(e)

    def get_save_open_close_data(self, openCloseURLs=None):
        """Fetch Daily Open Close from Polygon.io"""
        failed_tickers = []
        for URL in openCloseURLs:
            try:
                response = json.loads(requests.get(url=URL).text)
                symbol = response['ticker']
                if 'results' not in response:
                    continue
                response_data = [dict(item, **{'Symbol': symbol}) for item in response['results'] if 'results' in response]
                self.daily_open_close_object.insert_into_collection(symbol=symbol, datetosave=self.date,
                                                                    savedata=response_data[0],
                                                                    CollectionName=self.collection_name)
            except KeyError:
                print(f"################################ No results for {symbol} ################################")
                logger.warn(
                    f"################################ No results for {symbol} ################################")
            except Exception as e:
                print(e)
                print("Holding can't be fetched for URL =" + URL)
                traceback.print_exc()
                failed_tickers.append(URL.split('/')[6])
                logger.exception(e)
                logger.warning("Falling back to the latest available data")
                logger2.exception(e)
                logger2.warning("Falling back to the latest available data")
        return failed_tickers

    def fetch_data(self):
        """Fetch data from MongoDB"""
        try:
            return self.daily_open_close_object.fetch_daily_open_close_data(symbolList=self.symbols, date=self.date,
                                                                            CollectionName=self.collection_name)
        except Exception as e:
            logger.exception(e)
            logger2.exception(e)

    def run(self):
        """Main function to run Daily Open Close Operation"""
        try:
            symbols_to_be_downloaded = self.leave_existing_and_find_symbols_to_be_downloaded(symbols=self.symbols,
                                                                                             date=self.date,
                                                                                             CollectionName=self.collection_name)
            combined_data = []
            if len(symbols_to_be_downloaded) > 0:
                create_new_urls = self.create_urls(symbolsToBeDownloaded=symbols_to_be_downloaded)
                failed_tickers = self.get_save_open_close_data(openCloseURLs=create_new_urls)
                if failed_tickers:
                    if getpass.getuser() == 'ubuntu':
                        conn = MongoDBConnectors().get_pymongo_readonly_production_production()
                    else:
                        conn = MongoDBConnectors().get_pymongo_readonly_devlocal_production()
                    conn = MongoDBConnectors().get_pymongo_devlocal_devlocal()
                    pipeline = return_daily_open_close_pipeline(failed_tickers, self.date)
                    data_cursor = conn.ETF_db.DailyOpenCloseCollection.aggregate(pipeline)
                    combined_data = [data for data in data_cursor]
            data = self.fetch_data()
            data.extend(combined_data)
            return pd.DataFrame(data)
        except Exception as e:
            logger.exception(e)
            logger2.exception(e)
