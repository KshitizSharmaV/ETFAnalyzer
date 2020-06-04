import sys

sys.path.append("..")  # Remove in production - KTZ

import pandas as pd
from PolygonTickData.PolygonCreateURLS import PolgonDataCreateURLS
from CommonServices.ThreadingRequests import IOBoundThreading
from MongoDB.SaveFetchQuotesData import MongoDailyOpenCloseData
import traceback
import datetime
import requests
import json


class DailyOpenCloseData(object):

    def __init__(self, symbols=None, date=None, collectionName=None):
        self.symbols = symbols
        self.date = date
        self.collectionName = collectionName
        self.dailyopencloseObj = MongoDailyOpenCloseData()

    def createUrls(self, symbolsToBeDownloaded=None):

        endDate = datetime.datetime.strptime(self.date, '%Y-%m-%d') + datetime.timedelta(days=1)
        endDate = endDate.strftime('%Y-%m-%d')
        createUrls = PolgonDataCreateURLS()
        return [createUrls.PolygonAggregdateData(symbol=symbol, aggregateBy='day', startDate=self.date, endDate=endDate)
                for symbol in symbolsToBeDownloaded]

    # Used for threading
    def getSaveOpenCloseData(self, openCloseURLs=None):
        responses = IOBoundThreading(openCloseURLs)
        priceforNAVfilling = {}
        for response in responses:
            symbol = response['ticker']
            responseData = [dict(item, **{'Symbol': symbol}) for item in response['results']]
            try:
                self.dailyopencloseObj.insertIntoCollection(symbol=symbol, datetosave=self.date,
                                                            savedata=responseData[0],
                                                            CollectionName=self.collectionName)
            except Exception as e:
                print(e)
                print("Was not able to fetch data for Stock")

    # Used for ubthreading
    def getSaveOpenCloseDataNoThreading(self, openCloseURLs=None):
        priceforNAVfilling = {}
        for URL in openCloseURLs:
            try:
                response = json.loads(requests.get(url=URL).text)
                symbol = response['ticker']
                responseData = [dict(item, **{'Symbol': symbol}) for item in response['results']]
                self.dailyopencloseObj.insertIntoCollection(symbol=symbol, datetosave=self.date,
                                                            savedata=responseData[0],
                                                            CollectionName=self.collectionName)
            except Exception as e:
                print(e)
                print("Holding can't be fetched for URL =" + URL)
                traceback.print_exc()
                # Failure if any holding gave an issue
                return False
        # Success if holdings were scrapped success
        return True

    def fetchData(self):
        return self.dailyopencloseObj.fetchDailyOpenCloseData(symbolList=self.symbols, date=self.date,
                                                              CollectionName=self.collectionName)

    # Check if symbol,date pair exist in MongoDB, If don't exist download URLs for the symbols
    def checkIfDataExsistInMongoDB(self, symbols=None, date=None, CollectionName=None):
        symbolsToBeDownloaded = []
        for symbol in symbols:
            if not self.dailyopencloseObj.doesItemExsistInQuotesTradesMongoDb(symbol, date, CollectionName):
                symbolsToBeDownloaded.append(symbol)
        return symbolsToBeDownloaded

    def run(self):
        symbolsToBeDownloaded = self.checkIfDataExsistInMongoDB(symbols=self.symbols, date=self.date,
                                                                CollectionName=self.collectionName)

        if len(symbolsToBeDownloaded) > 0:
            # Create New URLS
            createNewUrls = self.createUrls(symbolsToBeDownloaded=symbolsToBeDownloaded)
            # Save data for those URls
            dailyDataStatus = self.getSaveOpenCloseDataNoThreading(openCloseURLs=createNewUrls)
            if not dailyDataStatus:
                return None

        data = self.fetchData()
        return pd.DataFrame(data)
