import sys
import time
sys.path.append('..')
import datetime
import pandas as pd

from CalculateETFArbitrage.LoadEtfHoldings import LoadHoldingsdata
from CommonServices.Holidays import HolidayCheck
from PolygonTickData.PolygonTradeQuotes import AssembleData
from MongoDB.Schemas import tradeCollection
from MongoDB.Schemas import tradespipeline

def getAndSaveTradesData(etfname, date):
    etfname = etfname
    date = date
    etfData = LoadHoldingsdata().LoadHoldingsAndClean(etfname=etfname, fundholdingsdate=date)
    ob = AssembleData(symbols=etfData.getSymbols(), date=date)
    tradesDataDf = ob.getData(CollectionName=tradeCollection, pipeline=tradespipeline, tradeDataFlag=True)
    tradesDataDf['Trade Price'] = (tradesDataDf['High Price'] + tradesDataDf['Low Price']) / 2
    print(tradesDataDf)
    print("SAVED {} {}".format(etfname,date))

def listOfHolidays():
    mydates = pd.date_range('2020-06-05', datetime.datetime.today().date().strftime("%Y-%m-%d")).tolist()
    print(mydates)
    myholiday_list = [date.date().strftime("%Y-%m-%d") for date in mydates if HolidayCheck(date)]
    return myholiday_list

def runTradesForAllETFs():
    start_time = time.time()
    etflist = list(pd.read_csv('../CSVFiles/250M_WorkingETFs.csv').columns)
    start = datetime.datetime.strptime("2020-07-24", "%Y-%m-%d")
    end = datetime.datetime.strptime("2020-07-25", "%Y-%m-%d")
    date_array = [start + datetime.timedelta(days=x) for x in range(0, (end - start).days)]
    datelist = [date.strftime('%Y-%m-%d') for date in date_array]
    list_of_holidays = listOfHolidays()
    for etf in etflist:
        for date in datelist:
            if date in list_of_holidays:
                continue
            else:
                getAndSaveTradesData(etf, date)
    end_time = time.time()
    print('JOB FINISHED IN : {} seconds'.format(end_time-start_time))

if __name__=='__main__':
    runTradesForAllETFs()