import sys  # Remove in production - KTZ

sys.path.append("..")  # Remove in production - KTZ
import json
import asyncio
from aiohttp import ClientSession
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

from MongoDB.SaveFetchQuotesData import MongoTradesQuotesData
from PolygonTickData.Helper import Helper

from CommonServices.LogCreater import CreateLogger

logger = CreateLogger().createLogFile(dirName='HistoricalArbitrage/', logFileName='-ArbEventLog.log',
                                      loggerName='TradesQuotesEventLogger')


class FetchPolygonData(object):
    """Fetch and Store Methods for Trades/Quotes Data from Polygon.io"""

    def __init__(self, date=None, end_time='21:00:00', end_time_loop='20:00:00', polygon_method=None,
                 symbol_status=None, collection_name=None, insert_into_collection=None):
        self.helperObj = Helper()
        self.date = date
        self.extract_data_till_time = self.helperObj.stringTimeToDatetime(date=date, time=end_time_loop)
        self.end_ts = self.helperObj.convertHumanTimeToUnixTimeStamp(date=date, time=end_time)
        self.polygon_method = polygon_method
        self.insert_into_collection = insert_into_collection
        self.collection_name = collection_name
        self.symbol_status = symbol_status

    def data_operation_runner(self, url):
        """Main for running Quotes data fetching operations"""
        pagination = url
        retry_counter = 2
        while pagination:
            response = self.response_from_api(pagination)
            while retry_counter > 0 and response['success'] == False:
                retry_counter -= 1
                print("Response failure from polygon")
                logger.error("Response failure from polygon")
                response = self.response_from_api(pagination)
            pagination_ = self.extract_data_from_response_and_store(response)
            pagination = pagination_ if pagination_ != pagination else None

    def response_from_api(self, url):
        """Get Quotes data from API URL"""
        session = requests.Session()
        retry = Retry(connect=3, backoff_factor=0.5)
        adapter = HTTPAdapter(max_retries=retry)
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        response = session.get(url)
        delay = response.headers.get("DELAY")
        date = response.headers.get("DATE")
        print("{}:{} with delay {}".format(date, response.url, delay))
        json_data = json.loads(response.text)
        return json_data


    def extract_data_from_response_and_store(self, response):
        """Extract data from response, check for pagination, store the data"""
        pagination_request = None
        symbol = response['ticker']
        print("Symbol being processed " + symbol)
        response_data = [dict(item, **{'Symbol': symbol}) for item in response['results'] if response['results']]
        last_unix_time_stamp = self.helperObj.getLastTimeStamp(response)
        print("Fetched till time for =" + str(self.helperObj.getHumanTime(last_unix_time_stamp)))
        logger.debug("Fetched till time for = {}".format(str(self.helperObj.getHumanTime(last_unix_time_stamp))))

        if self.helperObj.checkTimeStampForPagination(last_unix_time_stamp, self.extract_data_till_time):
            pagination_request = self.polygon_method(date=self.date, symbol=symbol, startTS=str(last_unix_time_stamp),
                                                     endTS=self.end_ts, limitresult=str(50000))
            self.symbol_status[symbol]['batchSize'] += 1

        _ = self.insert_into_collection(symbol=symbol, datetosave=self.date, savedata=response_data,
                                        CollectionName=self.collection_name,
                                        batchSize=self.symbol_status[symbol]['batchSize'])
        print("Pagination Request = {}".format(pagination_request))
        logger.debug("Pagination Request = {}".format(pagination_request))
        if pagination_request:
            return pagination_request
        else:
            print("No Pagination Required for = " + symbol)
            logger.debug("No Pagination Required for = {}".format(symbol))
            return None

    """Trades Get Operations"""

    async def get_trade_data_and_save(self, url, session):
        """Get Trades data from API URL Asynchronously"""
        try:
            async with session.get(url) as response:
                delay = response.headers.get("DELAY")
                date = response.headers.get("DATE")
                print("{}:{} with delay {}".format(date, response.url, delay))
                json_data = json.loads(await response.text())
                symbol = json_data['ticker']
                if 'results' not in json_data:
                    print(f"No results for {symbol}")
                data = [dict(item, **{'Symbol': symbol}) for item in json_data['results'] if 'results' in json_data]
                self.insert_into_collection(symbol=symbol, datetosave=self.date, savedata=data,
                                            CollectionName=self.collection_name)
                print(f'inserted {symbol} data')
        except KeyError:
            print(f"################################ No results for {symbol} ################################")
            logger.warn(f"################################ No results for {symbol} ################################")

    async def task_creator_and_gatherer(self, func, iterable):
        """Asynchronous Main for Trades data fetching operations"""
        async with ClientSession() as session:
            tasks = [asyncio.ensure_future(func(url, session)) for url in iterable]
            return await asyncio.gather(*tasks)
