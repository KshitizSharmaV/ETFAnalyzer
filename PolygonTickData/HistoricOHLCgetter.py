from pymongo import *
from CommonServices.ThreadingRequests import IOBoundThreading
from CommonServices.MultiProcessingTasks import CPUBonundThreading
from PolygonTickData.PolygonCreateURLS import PolgonDataCreateURLS
from dateutil.rrule import *
import datetime
import pandas as pd
import requests
import json

class HistoricOHLC():
    def getDailyOpenCloseSinceInceptionFromPolygon(self, getUrls=None):
        responses = IOBoundThreading(getUrls)
        return responses

    def getopenlowhistoric(self, etfname=None, startdate=None):
        todaysDate = datetime.datetime.today().strftime('%Y-%m-%d')
        url  = PolgonDataCreateURLS().PolygonAggregdateData(symbol=etfname, aggregateBy='day', startDate=startdate, endDate=todaysDate)
        response=requests.get(url)
        OHLCdata=pd.DataFrame(json.loads(response.text)['results'])
        return OHLCdata
