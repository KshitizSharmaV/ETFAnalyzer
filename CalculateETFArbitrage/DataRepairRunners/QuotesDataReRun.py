import sys
import time

sys.path.append('../..')
import datetime
import pandas as pd

from CalculateETFArbitrage.Helpers.LoadEtfHoldings import LoadHoldingsdata
from CommonServices.Holidays import HolidayCheck
from CalculateETFArbitrage.TradesQuotesRunner import TradesQuotesProcesses
from MongoDB.Schemas import quotesCollection
from MongoDB.Schemas import quotespipeline


def get_and_save_quotes_data(etfname, date):
    etfname = etfname
    date = date
    etfData = LoadHoldingsdata().LoadHoldingsAndClean(etfname=etfname, fundholdingsdate=date)
    ob = TradesQuotesProcesses(symbols=etfData.getSymbols(), date=date)
    ob.fetch_and_store_runner(collection_name=quotesCollection)
    quotesDataDf = ob.get_data(collection_name=quotesCollection, pipeline=quotespipeline)
    print(quotesDataDf)
    print("SAVED Quotes for {} {}".format(etfname, date))


def get_list_of_holidays():
    mydates = pd.date_range('2020-06-05', datetime.datetime.today().date().strftime("%Y-%m-%d")).tolist()
    print(mydates)
    myholiday_list = [date.date().strftime("%Y-%m-%d") for date in mydates if HolidayCheck(date)]
    return myholiday_list


def run_quotes_for_all_etfs():
    start_time = time.time()
    # etflist = list(pd.read_csv('../../CSVFiles/250M_WorkingETFs.csv').columns)
    etflist = ['SPY', 'VOO', 'QQQ', 'IVV', 'IJR', 'VO', 'VGT', 'XLK', 'XLF', 'SCHX']
    # For Date Range (start, end):
    # start = datetime.datetime.strptime("2020-07-24", "%Y-%m-%d")
    # end = datetime.datetime.strptime("2020-07-25", "%Y-%m-%d")
    # date_array = [start + datetime.timedelta(days=x) for x in range(0, (end - start).days)]
    # datelist = [date.strftime('%Y-%m-%d') for date in date_array]
    datelist = ['2021-09-07', '2021-09-08', '2021-09-09', '2021-09-10', '2021-09-13', '2021-09-14']
    datelist = ['2021-09-15']
    list_of_holidays = get_list_of_holidays()
    for etf in etflist:
        for date in datelist:
            if date in list_of_holidays:
                continue
            else:
                get_and_save_quotes_data(etf, date)
    end_time = time.time()
    print('JOB FINISHED IN : {} seconds'.format(end_time - start_time))


if __name__ == '__main__':
    run_quotes_for_all_etfs()
