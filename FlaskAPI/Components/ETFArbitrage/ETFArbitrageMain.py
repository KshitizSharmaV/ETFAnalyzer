from pymongo import MongoClient
import pandas as pd
import datetime
import time
import numpy as np
import talib
from pprint import pprint
import getpass
import socket
from MongoDB.MongoDBConnections import MongoDBConnectors
from FlaskAPI.Components.ETFArbitrage.helperForETFArbitrage import LoadETFPrices, LoadETFArbitrageData, \
    analysePerformance, countRightSignals
from FlaskAPI.Components.ETFArbitrage.MomentumSignal import MomentumSignals
from FlaskAPI.Components.ETFArbitrage.CandleStickPattern import PatternSignals

sys_private_ip = socket.gethostbyname(socket.gethostname())
if sys_private_ip == '172.31.76.32':
    connectionLocal = MongoDBConnectors().get_pymongo_readWrite_production_production()
else:
    connectionLocal = MongoDBConnectors().get_pymongo_readonly_devlocal_production()
connectionLocal = MongoDBConnectors().get_pymongo_devlocal_devlocal()
db = connectionLocal.ETF_db
TradesData = db.TradesData
arbitragecollection = db.ArbitrageCollectionNew

MomentumsignalsColumns = ['ADX Trend', 'AROONOSC Trend', 'Momentum Indicator', 'CMO Indicator',
                          'RSI Indicator', 'ULTOC Indicator', 'Stochastic Indicator', 'WILLR Indicator',
                          'MFI Indicator']

CandlesignalsColumns = ['Hammer Pat', 'InvertedHammer Pat', 'DragonFlyDoji Pat', 'PiercingLine Pat', 'MorningStar Pat',
                        '3WhiteSoldiers Pat', 'HanginMan Pat', 'Shooting Pat']

MajorUnderlyingMovers = ['ETFMover%1', 'ETFMover%2', 'ETFMover%3', 'ETFMover%4', 'ETFMover%5', 'ETFMover%6',
                         'ETFMover%7', 'ETFMover%8', 'ETFMover%9', 'ETFMover%10',
                         'Change%1', 'Change%2', 'Change%3', 'Change%4', 'Change%5', 'Change%6', 'Change%7', 'Change%8',
                         'Change%9', 'Change%10']

# 2 types of signals
# Signal Type 1 : When 111 = Sell and -111 = Buy
InverseSignal = ['FTEC']

# Signal Type 1 : When 111 = Buy and -111 = Sell
MaintainSignal = ['XLK', 'XLC', 'XLP']


def OverBoughtBalancedOverSold(df=None, magnitudeOfArbitrageToFilterOn=0):
    df['Magnitude of Arbitrage'] = abs(df['Arbitrage in $']) - df['ETF Trading Spread in $']
    # Replace all negative values with 0 
    df['Magnitude of Arbitrage'] = df['Magnitude of Arbitrage'].mask(df['Magnitude of Arbitrage'].lt(0), 0)
    df['Over Bought/Sold'] = 'Balanced'
    df.loc[(df['Magnitude of Arbitrage'] > magnitudeOfArbitrageToFilterOn) &
           (df['Arbitrage in $'] > 0), 'Over Bought/Sold'] = 'Over Bought'
    df.loc[(df['Magnitude of Arbitrage'] > magnitudeOfArbitrageToFilterOn) &
           (df['Arbitrage in $'] < 0), 'Over Bought/Sold'] = 'Over Sold'
    return df


# Calcualte Arbitrage and other results for a df
def calculateArbitrageResults(df=None, etfname=None, magnitudeOfArbitrageToFilterOn=0, BuildMomentumSignals=False,
                              BuildPatternSignals=False, includeMovers=True, getScatterPlot=True):
    # Get all signals if the etf is oversold or overbought
    df = OverBoughtBalancedOverSold(df=df, magnitudeOfArbitrageToFilterOn=magnitudeOfArbitrageToFilterOn)

    df = df.set_index('Time')

    # Build Signals if True passed by user, default is False
    df = MomentumSignals(df, tp=10) if BuildMomentumSignals else df
    df = PatternSignals(df) if BuildPatternSignals else df

    columnsneeded = ['ETF Trading Spread in $', 'Arbitrage in $', 'Magnitude of Arbitrage', 'Over Bought/Sold',
                     'ETF Price']
    # columnsneeded=columnsneeded+MomentumsignalsColumns+CandlesignalsColumns+MajorUnderlyingMovers
    columnsneeded = columnsneeded + MajorUnderlyingMovers if includeMovers else columnsneeded
    columnsneeded = columnsneeded + CandlesignalsColumns if BuildPatternSignals else columnsneeded

    allSignalsProcessing = pd.DataFrame()
    allSignalsProcessingTemp = analysePerformance(df=df, BuySellIndex=df)
    tempdf = df.loc[df.index]
    tempdf = tempdf[columnsneeded]
    allSignalsProcessingTemp = pd.merge(tempdf, allSignalsProcessingTemp, how='outer', left_index=True,
                                        right_index=True)
    # Dropt the last row
    allSignalsProcessingTemp.drop(allSignalsProcessingTemp.tail(1).index, inplace=True)
    allSignalsProcessing = allSignalsProcessing.append(allSignalsProcessingTemp)

    sellPositions = allSignalsProcessing.loc[allSignalsProcessing['Over Bought/Sold'] == 'Over Bought']
    PNLSellPositionsT_1 = round((sellPositions['T+1'].sum()), 2) if sellPositions.shape[0] != 0 else 0

    buyPositions = allSignalsProcessing.loc[allSignalsProcessing['Over Bought/Sold'] == 'Over Sold']
    PNLBuyPositionsT_1 = round(buyPositions['T+1'].sum(), 2) if buyPositions.shape[0] != 0 else 0

    scatterPlotData = df[['ETF Change Price %', 'Net Asset Value Change%']].to_dict(
        orient='records') if getScatterPlot else None

    pnlstatementforday = {'PNL% Sell Pos. (T+1)': PNLSellPositionsT_1, 'PNL% Buy Pos. (T+1)': PNLBuyPositionsT_1,
                          'Magnitue Of Arbitrage': magnitudeOfArbitrageToFilterOn}
    # allSignalsProcessing['Over Bought/Sold'] = allSignalsProcessing['Over Bought/Sold'].map({-111.0: 'Buy', 111.0: 'Sell',0:'No Action'})

    '''
    if etfname in MaintainSignal:
        pnlstatementforday = {'PNL% Sell Pos. (T+1)':-PNLBuyPositionsT_1,'PNL% Buy Pos. (T+1)':-PNLSellPositionsT_1,'Magnitue Of Arbitrage':magnitudeOfArbitrageToFilterOn}
        arbitrageBuySellSignals['Over Bought/Sold'] = arbitrageBuySellSignals['Over Bought/Sold'].map({-111.0: 'Sell', 111.0: 'Buy'})
        allSignalsProcessing['Over Bought/Sold'] = allSignalsProcessing['Over Bought/Sold'].map({-111.0: 'Sell', 111.0: 'Buy',0:'No Action'})
    else:
        pnlstatementforday = {'PNL% Sell Pos. (T+1)':PNLSellPositionsT_1,'PNL% Buy Pos. (T+1)':PNLBuyPositionsT_1,'Magnitue Of Arbitrage':magnitudeOfArbitrageToFilterOn}
        arbitrageBuySellSignals['Over Bought/Sold'] = arbitrageBuySellSignals['Over Bought/Sold'].map({111.0: 'Sell', -111.0: 'Buy'})
        allSignalsProcessing['Over Bought/Sold'] = allSignalsProcessing['Over Bought/Sold'].map({-111.0: 'Buy', 111.0: 'Sell',0:'No Action'})
    '''

    # Count the stats of Signal Right Buy, Right Sell, Total Buy & Total Sell
    resultDict = countRightSignals(allSignalsProcessing)
    pnlstatementforday = {**pnlstatementforday, **resultDict}

    return allSignalsProcessing, pnlstatementforday, scatterPlotData


# Collecs the data for arbitrage calculations
def AnalyzeArbitrageDataForETF(arbitrageDataFromMongo=None, magnitudeOfArbitrageToFilterOn=0):
    etfname = arbitrageDataFromMongo[0]['ETFName']
    pricedf = pd.DataFrame()

    dateOfAnalysis = arbitrageDataFromMongo[0]['dateOfAnalysis']
    year = dateOfAnalysis.year
    # Load Prices Data DF
    pricedf = LoadETFPrices(etfname, dateOfAnalysis, year, TradesData)
    # Load Arbitrage Data DF
    etfdata = LoadETFArbitrageData(arbitrageDataFromMongo, dateOfAnalysis, year)

    df = pd.merge(etfdata, pricedf, on='Time', how='left')
    df = df.ffill(axis=0)
    df.dropna(inplace=True)

    arbitrageBuySellSignals, pnlstatementforday, scatterPlotData = calculateArbitrageResults(df=df, etfname=etfname,
                                                                                             magnitudeOfArbitrageToFilterOn=magnitudeOfArbitrageToFilterOn)

    pricedf.columns = ['date', 'volume', 'open', 'close', 'high', 'low']
    return arbitrageBuySellSignals, pricedf, pnlstatementforday, scatterPlotData


# Historical arbitrage data for just 1 etf for 1 date
def RetrieveETFArbitrageData(etfname=None, date=None, magnitudeOfArbitrageToFilterOn=0):
    start = time.time()
    
    s = arbitragecollection.find({'ETFName': etfname, 'dateOfAnalysis': datetime.datetime.strptime(date, '%Y%m%d')},
                                 {'_id': 0})

    arbitrage_data = []                             
    for document in s:
        document['Time'] = document['Time']+datetime.timedelta(minutes=1)  # Time converted to end time.
        arbitrage_data.append(document)                        
    end = time.time()
    print(end-start)
    # Iter over the collection results - It's just 1 item
    PNLStatementForTheDay = {}
    allData, pricedf, pnlstatementforday, scatterPlotData = AnalyzeArbitrageDataForETF(arbitrageDataFromMongo=arbitrage_data,
                                                                                       magnitudeOfArbitrageToFilterOn=magnitudeOfArbitrageToFilterOn)
    PNLStatementForTheDay[str(arbitrage_data[0]['dateOfAnalysis'])] = pnlstatementforday
    endto = time.time()
    print(endto-end)

    return allData, pricedf, pnlstatementforday, scatterPlotData


# This function sends back PNL for all dates for ETF 
def retrievePNLForAllDays(etfname=None, magnitudeOfArbitrageToFilterOn=0):
    all_pnl_for_etf_cursor = db.PNLDataCollection.find({'Symbol': etfname}, {'_id': 0}).sort([('Date', -1)])
    all_pnl_for_etf = list(all_pnl_for_etf_cursor)
    all_pnl_for_etf = pd.DataFrame(all_pnl_for_etf)
    all_pnl_for_etf['Date'] = all_pnl_for_etf['Date'].apply(lambda x: datetime.datetime.strftime(x, '%Y-%m-%d'))
    all_pnl_for_etf.set_index('Date', inplace=True)
    del all_pnl_for_etf['Symbol']
    return all_pnl_for_etf.to_dict(orient='index')
