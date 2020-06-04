import sys

sys.path.append("..")  # Remove in production - KTZ

import pandas as pd
import datetime
import time

from PolygonTickData.FetchPolygonDataForUrls import FetchPolygonData
from PolygonTickData.Helper import Helper
from MongoDB.SaveFetchQuotesData import MongoTradesQuotesData
from PolygonTickData.PolygonCreateURLS import PolgonDataCreateURLS

class PolygonQuotesTradesData(object):
    def __init__(self):
        self.mtqd = MongoTradesQuotesData()

    # Check if symbol,date pair exist in MongoDB, If don't exist download URLs for the symbols
    def checkIfDataExsistInMongoDB(self, symbols=None, date=None, CollectionName=None):
        symbolsToBeDownloaded = []
        for symbol in symbols:
            if not self.mtqd.doesItemExsistInQuotesTradesMongoDb(symbol, date, CollectionName):
                symbolsToBeDownloaded.append(symbol)
        return symbolsToBeDownloaded

    # Fetch Data from MongoDb
    def fetchDataFromMongoDB(self, symbols=None, date=None, CollectionName=None, pipeline=None):
        print("Fetching Data")
        DictData = self.mtqd.fetchQuotesTradesDataFromMongo(symbolList=symbols, date=date, CollectionName=CollectionName, pipeline=pipeline)
        return pd.DataFrame(DictData)

    def createURLSforquotes(self, symbols=None, date=None, endTs=None):
        Routines = []
        symbolStatus = {}
        quotesUrls=PolgonDataCreateURLS()
        for symbol in symbols:
                Routines.append(quotesUrls.PolygonHistoricQuotes(date=date, symbol=symbol, startTS=None, endTS=endTs, limitresult=str(50000)))
                symbolStatus[symbol]={'batchSize':0}
        return Routines, symbolStatus

    def createURLSfortrade(self, symbols=None, startDate=None):
        Routines = []
        endDate=datetime.datetime.strptime(startDate,'%Y-%m-%d')+datetime.timedelta(days=1)
        endDate=endDate.strftime('%Y-%m-%d')
        tradeUrls=PolgonDataCreateURLS()
        for symbol in symbols:    
            Routines.append(tradeUrls.PolygonAggregdateData(symbol=symbol, aggregateBy='minute', startDate=startDate, endDate=startDate))
        return Routines


class AssembleData(object):
    def __init__(self, symbols=None, date=None):
        self.date = date
        self.endTs = Helper().convertHumanTimeToUnixTimeStamp(date=self.date, time='17:00:00')
        self.symbols = symbols
        self.obj = PolygonQuotesTradesData()

    def getData(self, pipeline=None, CollectionName=None, tradeDataFlag=False):
        # Perform check if symbols need to be downloaded or they already exist in the DB
        symbolsToBeDownloaded = self.obj.checkIfDataExsistInMongoDB(symbols=self.symbols, date=self.date, CollectionName=CollectionName)
        
        # Trade Configuration
        if symbolsToBeDownloaded and tradeDataFlag:
            Routines = self.obj.createURLSfortrade(symbols=symbolsToBeDownloaded, startDate=self.date)
            objFetchData = FetchPolygonData(date=self.date, CollectionName=CollectionName)
            storageAndCrawlingStatus = objFetchData.getTradeDataFromPolygon(getUrls=Routines)
            
        # Quotes Configuration
        elif symbolsToBeDownloaded:
            # Create URLs
            createUrl=PolgonDataCreateURLS().PolygonHistoricQuotes
            Routines, symbolStatus = self.obj.createURLSforquotes(symbols=symbolsToBeDownloaded, date=self.date, endTs=self.endTs)
            objFetchData = FetchPolygonData(date=self.date, PolygonMethod=createUrl, CollectionName=CollectionName, symbolStatus=symbolStatus)
            storageAndCrawlingStatus = objFetchData.getQuotesDataFromPolygon(getUrls=Routines)
            
            
        starttime=time.time()
        # Prepare to Return a dataframe for the Symbols
        resultdf = self.obj.fetchDataFromMongoDB(symbols=self.symbols, date=self.date, CollectionName=CollectionName, pipeline=pipeline)
        print("-------%s-------" %(time.time() - starttime))
        return resultdf
