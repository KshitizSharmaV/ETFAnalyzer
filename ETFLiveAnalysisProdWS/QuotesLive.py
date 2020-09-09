import sys, traceback
sys.path.append("..")
import datetime
import pandas as pd
from CommonServices.MultiProcessingTasks import CPUBonundThreading
from CommonServices.ThreadingRequests import IOBoundThreading
from CommonServices.LogCreater import  CreateLogger
logger = CreateLogger().createLogFile(dirName='ETFLiveAnalysis/', logFileName='-QuotesLiveFetchLog.log', loggerName='QuotesLiveFetch')

from PolygonTickData.PolygonCreateURLS import PolgonDataCreateURLS
from MongoDB.PerMinDataOperations import PerMinDataOperations

class QuotesLiveFetcher():
    def __init__(self):
        self.etflist = pd.read_csv('../CSVFiles/250M_WorkingETFs.csv').columns.to_list()
        self.getUrls = [PolgonDataCreateURLS().PolygonLastQuotes(etf) for etf in self.etflist]

    def getDataFromPolygon(self, methodToBeCalled=None, getUrls=None):
        # Calling IO Bound Threading to fetch data for URLS
        if methodToBeCalled == None or getUrls == None:
            logger.debug('Either methodToBeCalled or getUrls not supplied')
            return None
        responses = IOBoundThreading(getUrls)
        ResultsfromResponses = CPUBonundThreading(methodToBeCalled, responses)
        return ResultsfromResponses

    def extractQuotesDataFromResponses(self, response):
        try:
            responseData = {"symbol":response['symbol']}
            responseData.update(**response['last'])
        except:
            #print("No quotes data for {}".format(response['symbol']))
            logger.debug("No quotes data for {}".format(response['symbol']))
            responseData = None
        return responseData

    def fetch_save_Last_Quotes(self):
        try:
            #print("Fetching")
            responseData = self.getDataFromPolygon(methodToBeCalled=self.extractQuotesDataFromResponses, getUrls=self.getUrls)
            responseData = list(responseData)
            #print("Inserting")
            #print(datetime.datetime.now())
            #print(responseData)
            PerMinDataOperations().insertQuotesLive(quotesData=responseData)
            #print("Inserted Successfully")
            logger.debug("Quotes Inserted")
        except  Exception as e:
            #print("Insertion failed")
            logger.exception(e)
            pass

if __name__=='__main__':
    QuotesLiveObj = QuotesLiveFetcher()
    while True:
        QuotesLiveObj.fetch_save_Last_Quotes()