# Gathers Data from APIs
# 1) Trade Data
# 2) Quotes Data
# 3) Daily/Open Close Data
# 4) ETFDB Data
import sys

sys.path.append("..")  # Remove in production - KTZ

import pandas as pd
from PolygonTickData.PolygonTradeQuotes import AssembleData
# Import Collections
from MongoDB.Schemas import quotesCollection, tradeCollection, dailyopencloseCollection
# Import Pipeline
from MongoDB.Schemas import quotespipeline, tradespipeline
from PolygonTickData.DataDailyOpenClose import DailyOpenCloseData


class DataApi(object):

    def __init__(self, etfname=None, date=None, etfData=None):
        self.etfname = etfname
        self.date = date
        self.etfData = etfData

        # Object for Trade Data
        self.tradesDataDf = self.gatherTradeData()
        # Object for Quotes Data
        self.quotesDataDf = self.gatherQuotesData()
        # Object for Open Close Data
        self.openPriceData = self.gatherOpenCloseData()

    def gatherTradeData(self):
        ob = AssembleData(symbols=self.etfData.getSymbols(), date=self.date)
        tradesDataDf = ob.getData(CollectionName=tradeCollection, pipeline=tradespipeline, tradeDataFlag=True)
        tradesDataDf['Trade Price'] = (tradesDataDf['Open Price'] + tradesDataDf['Close Price']) / 2
        return tradesDataDf

    def gatherQuotesData(self):
        ob = AssembleData(symbols=[self.etfname], date=self.date)
        quotesDataDf = ob.getData(CollectionName=quotesCollection, pipeline=quotespipeline)
        return quotesDataDf

    def gatherOpenCloseData(self):
        return DailyOpenCloseData(symbols=self.etfData.getSymbols(), date=self.date,
                                  collectionName=dailyopencloseCollection).run()
