import sys  # Remove in production - KTZ

sys.path.append("..")  # Remove in production - KTZ

import pandas as pd
import logging
from functools import reduce
import datetime
import numpy as np

from PolygonTickData.Helper import Helper
from CalculateETFArbitrage.LoadEtfHoldings import LoadHoldingsdata
from CalculateETFArbitrage.GatherData import DataApi


class ArbitrageCalculation():
    def calculateArbitrage(self, etfname, date):
        # Create an object of ETF Hoding Data
        etfname = etfname
        date = date

        # Load the ETF Holding
        etfData = LoadHoldingsdata().LoadHoldingsAndClean(etfname=etfname, fundholdingsdate=date)

        # Load all the data - Holdings data for Etf, trade data, quotes data, open-close price
        allData = DataApi(etfname=etfname, date=date, etfData=etfData)

        # Check if any holdings is trading in Non-US markets
        if allData.openPriceData is None:
            return None

        # Convert Time Stamps to Pandas Timestamp
        helperObj = Helper()
        allData.tradesDataDf['Time'] = allData.tradesDataDf['Time'].apply(lambda x: helperObj.getHumanTime(ts=x, divideby=1000))
        tradePricesDFMinutes = allData.tradesDataDf.groupby([allData.tradesDataDf['Time'], allData.tradesDataDf['Symbol']])['Trade Price']
        tradePricesDFMinutes = tradePricesDFMinutes.first().unstack(level=1)

        priceforNAVfilling = allData.openPriceData.set_index('Symbol').T.to_dict('records')[0]

        marketStartTime = datetime.datetime.strptime('13:29:00', "%H:%M:%S").time()
        marketEndTime = datetime.datetime.strptime('20:00:00', "%H:%M:%S").time()

        mask = (tradePricesDFMinutes.index.time >= marketStartTime) & (tradePricesDFMinutes.index.time <= marketEndTime)
        tradePricesDFMinutes=tradePricesDFMinutes[mask]
        tradePricesDFMinutes=tradePricesDFMinutes.ffill(axis=0)
        tradePricesDFMinutes = tradePricesDFMinutes.fillna(priceforNAVfilling)

        etfprice = tradePricesDFMinutes[etfname]
        tradePricesDFMinutes = tradePricesDFMinutes.pct_change().dropna() * 100

        etfpricechange = tradePricesDFMinutes[etfname]
        del tradePricesDFMinutes[etfname]

        allData.quotesDataDf['Time'] = allData.quotesDataDf['Time'].apply(lambda x: helperObj.getHumanTime(ts=x, divideby=1000000000))
        allData.quotesDataDf['Time'] = allData.quotesDataDf['Time'].map(lambda x: x.replace(second=0))
        allData.quotesDataDf = allData.quotesDataDf[allData.quotesDataDf['Bid Size'] != 0]
        allData.quotesDataDf = allData.quotesDataDf[allData.quotesDataDf['Ask Size'] != 0]
        allData.quotesDataDf['Total Bid Ask Size'] = allData.quotesDataDf['Ask Size'] + allData.quotesDataDf['Bid Size']
        allData.quotesDataDf['Spread'] = allData.quotesDataDf['Ask Price'] - allData.quotesDataDf['Bid Price']
        allData.quotesDataDf['MidPrice'] = (allData.quotesDataDf['Ask Price'] + allData.quotesDataDf['Bid Price']) / 2
        quotesSpreadsMinutes = allData.quotesDataDf.groupby('Time')['Spread'].mean()

        netassetvaluereturn = tradePricesDFMinutes.assign(**etfData.getETFWeights()).mul(tradePricesDFMinutes).sum(axis=1)
        ds = pd.concat([etfprice, etfpricechange, netassetvaluereturn, quotesSpreadsMinutes], axis=1).dropna()
        ds.columns = ['ETF Price', 'ETF Change Price %', 'Net Asset Value Change%', 'ETF Trading Spread in $']
        ds['Arbitrage in $'] = (ds['ETF Change Price %'] - ds['Net Asset Value Change%']) * ds['ETF Price'] / 100
        ds['Flag'] = 0
        ds.loc[(abs(ds['Arbitrage in $']) > ds['ETF Trading Spread in $']) & ds['ETF Trading Spread in $'] != 0, 'Flag'] = 111
        ds['Flag'] = ds['Flag'] * np.sign(ds['Arbitrage in $'])

        holdingsChange = helperObj.EtfMover(df=tradePricesDFMinutes, columnName='Change%')
        etfMoverholdings = helperObj.EtfMover(df=tradePricesDFMinutes.assign(**etfData.getETFWeights()).mul(tradePricesDFMinutes).dropna(axis=1), columnName='ETFMover%')

        ds=pd.concat([ds,etfMoverholdings,holdingsChange],axis=1)

        print(ds)
        
        return ds


