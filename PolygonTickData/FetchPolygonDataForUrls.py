import sys  # Remove in production - KTZ

sys.path.append("..")  # Remove in production - KTZ

from PolygonTickData.PolygonCreateURLS import PolgonDataCreateURLS
from PolygonTickData.Helper import Helper
from CalculateETFArbitrage.LoadEtfHoldings import LoadHoldingsdata
from CommonServices.ThreadingRequests import IOBoundThreading
from CommonServices.MultiProcessingTasks import CPUBonundThreading
from MongoDB.SaveFetchQuotesData import MongoTradesQuotesData
from datetime import datetime
import logging
import os

path = os.path.join(os.getcwd(), "Logs/")
if not os.path.exists(path):
    os.makedirs(path)

filename = path + datetime.now().strftime("%Y%m%d") + "-ArbEventLog.log"
handler = logging.FileHandler(filename)
logging.basicConfig(filename=filename, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', filemode='w')
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.addHandler(handler)

import asyncio
import datetime
import pandas as pd
import sys
import time


class FetchPolygonData(object):

    def __init__(self, date=None, previousdate=None, starttime='12:00:00', endtime='21:00:00', endtimeLoop='20:00:00',
                 PolygonMethod=None, symbolStatus=None, CollectionName=None):
        self.helperObj = Helper()
        self.date = date
        self.starttime = starttime  # 9 AM
        self.endtime = endtime  # 5 PM
        self.endtimeLoop = endtimeLoop  # 4 PM
        self.extractDataTillTime = self.helperObj.stringTimeToDatetime(date=self.date, time=self.endtimeLoop)
        self.endTs = self.helperObj.convertHumanTimeToUnixTimeStamp(date=self.date, time=self.endtime)
        self.PolygonMethod = PolygonMethod
        self.insertIntoCollection = MongoTradesQuotesData().insertIntoCollection
        self.CollectionName = CollectionName
        self.symbolStatus = symbolStatus

    def __extractDataFromResponse(self, response):
        PaginationRequest = None
        symbol = response['ticker']
        print("Symbol being processed " + symbol)
        responseData = [dict(item, **{'Symbol': symbol}) for item in response['results']]
        lastUnixTimeStamp = self.helperObj.getLastTimeStamp(response)
        print("Fetched till time for =" + str(self.helperObj.getHumanTime(lastUnixTimeStamp)))
        logger.debug("Fetched till time for = {}".format(str(self.helperObj.getHumanTime(lastUnixTimeStamp))))
        if self.helperObj.checkTimeStampForPagination(lastUnixTimeStamp, self.extractDataTillTime):
            # Create new urls for pagination request
            PaginationRequest = self.PolygonMethod(date=self.date, symbol=symbol, startTS=str(lastUnixTimeStamp),
                                                   endTS=self.endTs, limitresult=str(50000))
            self.symbolStatus[symbol]['batchSize'] += 1
        # Creating an efficient storage object with PolygonResponseStorage for returning
        _ = self.insertIntoCollection(symbol=symbol, datetosave=self.date, savedata=responseData,
                                      CollectionName=self.CollectionName,
                                      batchSize=self.symbolStatus[symbol]['batchSize'])
        print("Pagination Request = {}".format(PaginationRequest))
        logger.debug("Pagination Request = {}".format(PaginationRequest))
        # if not PaginationRequest and PaginationRequest==
        if PaginationRequest:
            return PaginationRequest
        else:
            print("No Pagination Required for = " + symbol)
            logger.debug("No Pagination Required for = {}".format(symbol))
            return None

    def getQuotesDataFromPolygon(self, getUrls=None):
        # Calling IO Bound Threading to fetch data for URLS
        responses = IOBoundThreading(getUrls)
        # Calling CPU Bound Threading to proess the responses from URLS
        ThreadingResults = CPUBonundThreading(self.__extractDataFromResponse, responses)
        PaginationRequest = [paginationUrl for paginationUrl in ThreadingResults if paginationUrl]
        # Check if we need to do pagination for results, if Yes we do a recursion call to getDataFromPolygon
        if len(PaginationRequest) > 0 and PaginationRequest[0] != getUrls[0]:
            _ = self.getQuotesDataFromPolygon(getUrls=PaginationRequest)
        return True

    def getTradeDataFromPolygon(self, getUrls=None):
        # Calling IO Bound Threading to fetch data for URLS
        responses = IOBoundThreading(getUrls)
        for response in responses:
            symbol = response['ticker']
            responseData = [dict(item, **{'Symbol': symbol}) for item in response['results']]
            _ = self.insertIntoCollection(symbol=symbol, datetosave=self.date, savedata=responseData,
                                          CollectionName=self.CollectionName)
        return True
