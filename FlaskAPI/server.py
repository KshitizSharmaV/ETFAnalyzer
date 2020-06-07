import sys
from flask import Flask, request, jsonify
from flask_cors import CORS
from mongoengine import *
import sys
import json
import pandas as pd
from mongoengine import connect
import numpy as np
import math
import ast
import json
from datetime import datetime, timedelta
import traceback
import sys
import getpass

sys.path.append("..")
app = Flask(__name__)

CORS(app)

from MongoDB.MongoDBConnections import MongoDBConnectors
system_username = getpass.getuser()
if system_username == 'ubuntu':
    connection = MongoDBConnectors().get_pymongo_readWrite_production_production()
else:
    connection = MongoDBConnectors().get_pymongo_readonly_devlocal_production()
############################################
# ETF Description Page
############################################


from FlaskAPI.Components.ETFDescription.helper import fetchETFsWithSameIssuer, fetchETFsWithSameETFdbCategory, \
    fetchETFsWithSimilarTotAsstUndMgmt, fetchOHLCHistoricalData
from CalculateETFArbitrage.LoadEtfHoldings import LoadHoldingsdata


@app.route('/ETfDescription/getETFWithSameIssuer/<IssuerName>')
def getETFWithSameIssuer(IssuerName):
    etfswithsameIssuer = fetchETFsWithSameIssuer(connection, Issuer=IssuerName)
    if len(etfswithsameIssuer) == 0:
            etfswithsameIssuer['None'] = {'ETFName': 'None','TotalAssetsUnderMgmt': "No Other ETF was found with same Issuer"}
    return json.dumps(etfswithsameIssuer)

@app.route('/ETfDescription/getETFsWithSameETFdbCategory/<ETFdbCategory>')
def getETFsWithSameETFdbCategory(ETFdbCategory):
    etfsWithSameEtfDbCategory = fetchETFsWithSameETFdbCategory(connection=connection,ETFdbCategory=ETFdbCategory)
    if len(etfsWithSameEtfDbCategory) == 0:
            etfsWithSameEtfDbCategory['None'] = {'ETFName': 'None','TotalAssetsUnderMgmt': "No Other ETF was found with same ETF DB Category"}
    return json.dumps(etfsWithSameEtfDbCategory)

@app.route('/ETfDescription/getOHLCDailyData/<ETFName>/<StartDate>')
def fetchOHLCDailyData(ETFName,StartDate):
    StartDate=StartDate.split(' ')[0]
    OHLCData=fetchOHLCHistoricalData(etfname=ETFName,StartDate=StartDate)
    OHLCData=OHLCData.to_csv(sep='\t', index=False)
    return OHLCData



@app.route('/ETfDescription/EtfData/<ETFName>/<date>')
def SendETFHoldingsData(ETFName, date):
    req = request.__dict__['environ']['REQUEST_URI']
    try:
        # connect_mongoengine = MongoDBConnectors().get_mongoengine_readonly_devlocal_production()
        MongoDBConnectors().get_mongoengine_readonly_devlocal_production()
        # Load all the data holdings data together
        etfdata = LoadHoldingsdata().getAllETFData(ETFName, date)
        ETFDataObject = etfdata.to_mongo().to_dict()
        print(ETFDataObject)
        HoldingsDatObject=pd.DataFrame(ETFDataObject['holdings']).set_index('TickerSymbol').T.to_dict()
        SimilarTotalAsstUndMgmt = fetchETFsWithSimilarTotAsstUndMgmt(connection=connection,totalassetUnderManagement=ETFDataObject['TotalAssetsUnderMgmt'])

        ETFDataObject['TotalAssetsUnderMgmt']="${:,.3f} M".format(ETFDataObject['TotalAssetsUnderMgmt']/1000)
        ETFDataObject['SharesOutstanding']="{:,.0f}".format(ETFDataObject['SharesOutstanding'])
        ETFDataObject['InceptionDate'] = str(ETFDataObject['InceptionDate'])
        
        
        # List of columns we don't need
        for v in ['_id', 'DateOfScraping', 'ETFhomepage', 'holdings','FundHoldingsDate']:
            del ETFDataObject[v]
        
        ETFDataObject = pd.DataFrame(ETFDataObject, index=[0])
        ETFDataObject = ETFDataObject.replace(np.nan, 'nan', regex=True)
        ETFDataObject = ETFDataObject.loc[0].to_dict()
        
        
        allData = {}
        allData['ETFDataObject'] = ETFDataObject
        allData['HoldingsDatObject'] = HoldingsDatObject
        allData['SimilarTotalAsstUndMgmt'] = SimilarTotalAsstUndMgmt

        print(ETFDataObject)
        print(allData['HoldingsDatObject'])
        print(SimilarTotalAsstUndMgmt)

        return json.dumps(allData)

    except Exception as e:
        print("Issue in Flask app while fetching ETF Description Data")
        print(traceback.format_exc())
        return str(e)


############################################
# Historical Arbitrage
############################################
from FlaskAPI.Components.ETFArbitrage.ETFArbitrageMain import RetrieveETFArbitrageData, retrievePNLForAllDays

# Divide Columnt into movers and the price by which they are moving
etmoverslist = ['ETFMover%1', 'ETFMover%2', 'ETFMover%3', 'ETFMover%4', 'ETFMover%5',
                'ETFMover%6', 'ETFMover%7', 'ETFMover%8', 'ETFMover%9', 'ETFMover%10',
                'Change%1', 'Change%2', 'Change%3', 'Change%4', 'Change%5', 'Change%6',
                'Change%7', 'Change%8', 'Change%9', 'Change%10']


@app.route('/PastArbitrageData/<ETFName>/<date>')
def FetchPastArbitrageData(ETFName, date):
    ColumnsForDisplay = ['$Spread', '$Arbitrage', 'Absolute Arbitrage',
                         'Over Bought/Sold',
                         'Etf Mover',
                         'Most Change%',
                         'T', 'T+1']

    # Retreive data for Components
    data, pricedf, PNLStatementForTheDay, scatterPlotData = RetrieveETFArbitrageData(etfname=ETFName, date=date,
                                                                                     magnitudeOfArbitrageToFilterOn=0)

    # Check if data doesn't exsist
    if data.empty:
        print("No Data Exist")

    ########### Code to modify the ETF Movers and Underlying with highest change %
    # Seperate ETF Movers and the percentage of movement
    for movers in etmoverslist:
        def getTickerReturnFromMovers(x):
            # x = ast.literal_eval(x)
            return x[0], float(x[1])

        newcolnames = [movers + '_ticker', movers + '_value']
        data[movers] = data[movers].apply(getTickerReturnFromMovers)
        data[newcolnames] = pd.DataFrame(data[movers].tolist(), index=data.index)
        del data[movers]

    etfmoversList = dict(data[['ETFMover%1_ticker', 'ETFMover%2_ticker', 'ETFMover%3_ticker']].stack().value_counts())
    etfmoversDictCount = pd.DataFrame.from_dict(etfmoversList, orient='index', columns=['Count']).to_dict('index')

    highestChangeList = dict(data[['Change%1_ticker', 'Change%2_ticker', 'Change%3_ticker']].stack().value_counts())
    highestChangeDictCount = pd.DataFrame.from_dict(highestChangeList, orient='index', columns=['Count']).to_dict(
        'index')
    ########## Code to modify the ETF Movers and Underlying with highest change %

    # Sort the data frame on time since Sell and Buy are concatenated one after other
    data = data.sort_index()

    # Time Manpulation
    data.index = data.index.time
    data.index = data.index.astype(str)

    # Round of DataFrame 
    data = data.round(3)
    print(data.head())

    # Replace Values in Pandas DataFrame
    data.rename(columns={'ETF Trading Spread in $': '$Spread',
                         'Arbitrage in $': '$Arbitrage',
                         'Magnitude of Arbitrage': 'Absolute Arbitrage',
                         'ETFMover%1_ticker': 'Etf Mover',
                         'Change%1_ticker': 'Most Change%'}, inplace=True)

    # Get the price dataframe
    allData = {}
    # Columns needed to display
    data = data[ColumnsForDisplay]

    # PNL for all dates for the etf
    allData['etfhistoricaldata'] = data.to_json(orient='index')
    print("Price Df")
    print(pricedf)
    allData['etfPrices'] = pricedf.to_csv(sep='\t', index=False)
    allData['PNLStatementForTheDay'] = json.dumps(PNLStatementForTheDay)
    allData['scatterPlotData'] = json.dumps(scatterPlotData)
    allData['etfmoversDictCount'] = json.dumps(etfmoversDictCount)
    allData['highestChangeDictCount'] = json.dumps(highestChangeDictCount)
    return json.dumps(allData)


@app.route('/PastArbitrageData/CommonDataAcrossEtf/<ETFName>')
def fetchPNLForETFForALlDays(ETFName):
    print("All ETF PNL Statement is called")
    PNLOverDates = retrievePNLForAllDays(etfname=ETFName, magnitudeOfArbitrageToFilterOn=0)
    allData = {}
    print(PNLOverDates)
    allData['PNLOverDates'] = json.dumps(PNLOverDates)
    return allData


############################################
# Live Arbitrage All ETFs
############################################

from MongoDB.PerMinDataOperations import PerMinDataOperations


@app.route('/ETfLiveArbitrage/AllTickers')
def SendLiveArbitrageDataAllTickers():
    req = request.__dict__['environ']['REQUEST_URI']
    try:
        live_data = PerMinDataOperations().FetchPerMinLiveData()
        live_prices = PerMinDataOperations().FetchAllETFPricesLive()

        live_prices_data = []
        [live_prices_data.append(item) for item in live_prices]

        data2 = []
        [data2.extend(item['ArbitrageData']) for item in live_data]

        prices_df = pd.DataFrame.from_records(live_prices_data)
        prices_df.rename(columns={'sym': 'Symbol', 'vw': 'Price', 'e': 'Timestamp'}, inplace=True)
        df = pd.DataFrame.from_records(data2)
        ndf = df.merge(prices_df, how='left', on='Symbol')
        ndf.dropna(inplace=True)
        return ndf.to_dict()
    except Exception as e:
        print("Issue in Flask app while fetching ETF Description Data")
        print(traceback.format_exc())
        return str(e)


############################################
# Live Arbitrage Single ETF
############################################
import time
from FlaskAPI.Components.LiveCalculations.helperLiveArbitrage import fecthArbitrageANDLivePrices, analyzeSignalPerformane, AnalyzeDaysPerformance


@app.route('/ETfLiveArbitrage/Single/<etfname>')
def SendLiveArbitrageDataSingleTicker(etfname):
    PerMinObj = PerMinDataOperations()
    res = fecthArbitrageANDLivePrices(etfname=etfname, FuncETFPrices=PerMinObj.FetchFullDayPricesForETF, FuncArbitrageData=PerMinObj.FetchFullDayPerMinArbitrage)
    print(res['Prices'])
    print(res['Arbitrage'])
    res['Prices']=res['Prices'].to_csv(sep='\t', index=False)
    #res['DaysPerformance']=AnalyzeDaysPerformance(res['Arbitrage'])
    res['pnlstatementforday'] = AnalyzeDaysPerformance(ArbitrageDf=res['Arbitrage'],etfname=etfname)
    res['Arbitrage']=res['Arbitrage'].to_dict()
    return res


@app.route('/ETfLiveArbitrage/Single/UpdateTable/<etfname>')
def UpdateLiveArbitrageDataTablesAndPrices(etfname):
    PerMinObj = PerMinDataOperations()
    res = fecthArbitrageANDLivePrices(etfname=etfname, FuncETFPrices=PerMinObj.LiveFetchETFPrice, FuncArbitrageData=PerMinObj.LiveFetchPerMinArbitrage)
    print(res['Prices'])
    print(res['Arbitrage'])
    res['Prices']=res['Prices'].to_dict()
    res['Arbitrage']=res['Arbitrage'].to_dict()
    res['SignalInfo']=analyzeSignalPerformane(res['Arbitrage']['Arbitrage'][0])
    return res

if __name__ == '__main__':
    app.run(port=5000, debug=True)
