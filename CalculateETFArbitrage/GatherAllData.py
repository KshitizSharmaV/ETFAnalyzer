# Gathers Data from APIs
# 1) Trade Data
# 2) Quotes Data
# 3) Daily/Open Close Data
# 4) ETFDB Data
import sys

sys.path.append("..")  # Remove in production - KTZ
import pandas as pd
from CalculateETFArbitrage.TradesQuotesRunner import TradesQuotesProcesses
# Import Collections
from MongoDB.Schemas import quotesCollection, tradeCollection, dailyopencloseCollection
# Import Pipeline
from MongoDB.Schemas import quotespipeline, tradespipeline
from CalculateETFArbitrage.FetchAndProcessDailyOpenClose import DailyOpenCloseData


class DataApi(object):

    def __init__(self, etfname=None, date=None, etfData=None):
        self.etfname = etfname
        self.date = date
        self.etfData = etfData
        '''Object for Trade Data'''
        self.tradesDataDf = pd.DataFrame()
        '''Object for Quotes Data'''
        self.quotesDataDf = pd.DataFrame()
        '''Object for Open Close Data'''
        self.openPriceData = pd.DataFrame()

    def gatherTradeData(self):
        ob = TradesQuotesProcesses(symbols=self.etfData.getSymbols(), date=self.date)
        ob.fetch_and_store_runner(collection_name=tradeCollection, trade_data_flag=True)
        self.tradesDataDf = ob.get_data(collection_name=tradeCollection, pipeline=tradespipeline)
        self.tradesDataDf['Trade Price'] = (self.tradesDataDf['Open Price'] + self.tradesDataDf['Close Price']) / 2
        return self.tradesDataDf

    def gatherQuotesData(self):
        ob = TradesQuotesProcesses(symbols=[self.etfname], date=self.date)
        ob.fetch_and_store_runner(collection_name=quotesCollection)
        self.quotesDataDf = ob.get_data(collection_name=quotesCollection, pipeline=quotespipeline)
        return self.quotesDataDf

    def gatherOpenCloseData(self):
        self.openPriceData = DailyOpenCloseData(symbols=self.etfData.getSymbols(), date=self.date,
                                                collection_name=dailyopencloseCollection).run()
        return self.openPriceData

    def run_all_data_ops(self):
        self.gatherTradeData()
        self.gatherQuotesData()
        self.gatherOpenCloseData()
