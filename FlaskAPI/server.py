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

@app.route('/ETfDescription/getHoldingsData/<ETFName>/<StartDate>')
def fetchHoldingsData(ETFName,StartDate):
    MongoDBConnectors().get_mongoengine_readonly_devlocal_production()
    print("fetchHoldingsData calle")
    etfdata = LoadHoldingsdata().getAllETFData(ETFName, StartDate)
    ETFDataObject = etfdata.to_mongo().to_dict()
    HoldingsDatObject=pd.DataFrame(ETFDataObject['holdings']).set_index('TickerSymbol').round(2).T.to_dict()
    print(HoldingsDatObject)
    return HoldingsDatObject

@app.route('/ETfDescription/EtfData/<ETFName>/<date>')
def SendETFHoldingsData(ETFName, date):
    print("date**********")
    print(date)
    req = request.__dict__['environ']['REQUEST_URI']
    try:
        allData = {}
        MongoDBConnectors().get_mongoengine_readonly_devlocal_production()
        etfdata = LoadHoldingsdata().getAllETFData(ETFName, date)
        ETFDataObject = etfdata.to_mongo().to_dict()
        
        allData['SimilarTotalAsstUndMgmt'] = fetchETFsWithSimilarTotAsstUndMgmt(connection=connection,totalassetUnderManagement=ETFDataObject['TotalAssetsUnderMgmt'])

        ETFDataObject['TotalAssetsUnderMgmt']="${:,.3f} M".format(ETFDataObject['TotalAssetsUnderMgmt']/1000)
        ETFDataObject['SharesOutstanding']="{:,.0f}".format(ETFDataObject['SharesOutstanding'])
        ETFDataObject['InceptionDate'] = str(ETFDataObject['InceptionDate'])
        # List of columns we don't need
        for v in ['_id', 'DateOfScraping', 'ETFhomepage', 'holdings','FundHoldingsDate']:
            del ETFDataObject[v]
        ETFDataObject = pd.DataFrame(ETFDataObject, index=[0])
        ETFDataObject = ETFDataObject.replace(np.nan, 'nan', regex=True)
        ETFDataObject = ETFDataObject.loc[0].to_dict()
        
        allData['ETFDataObject'] = ETFDataObject
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
    print(date)
    print(type(date))
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
    # PNLOverDates ={'2020-04-01': {'Sell Return%': 1.64, 'Buy Return%': 0.86, '# T.Buy': 106.0, '# R.Buy': 53.0, '% R.Buy': 0.5, '# T.Sell': 126.0, '# R.Sell': 75.0, '% R.Sell': 0.6}, '2020-04-02': {'Sell Return%': 4.03, 'Buy Return%': 5.19, '# T.Buy': 119.0, '# R.Buy': 75.0, '% R.Buy': 0.63, '# T.Sell': 128.0, '# R.Sell': 80.0, '% R.Sell': 0.62}, '2020-04-03': {'Sell Return%': 2.34, 'Buy Return%': 1.39, '# T.Buy': 101.0, '# R.Buy': 54.0, '% R.Buy': 0.53, '# T.Sell': 110.0, '# R.Sell': 68.0, '% R.Sell': 0.62}, '2020-04-06': {'Sell Return%': 0.55, 'Buy Return%': 3.16, '# T.Buy': 95.0, '# R.Buy': 63.0, '% R.Buy': 0.66, '# T.Sell': 64.0, '# R.Sell': 33.0, '% R.Sell': 0.52}, '2020-04-07': {'Sell Return%': 3.73, 'Buy Return%': 2.4, '# T.Buy': 115.0, '# R.Buy': 72.0, '% R.Buy': 0.63, '# T.Sell': 132.0, '# R.Sell': 79.0, '% R.Sell': 0.6}, '2020-04-08': {'Sell Return%': 0.82, 'Buy Return%': 2.02, '# T.Buy': 95.0, '# R.Buy': 56.0, '% R.Buy': 0.59, '# T.Sell': 103.0, '# R.Sell': 54.0, '% R.Sell': 0.52}, '2020-04-09': {'Sell Return%': 2.15, 'Buy Return%': 1.98, '# T.Buy': 110.0, '# R.Buy': 65.0, '% R.Buy': 0.59, '# T.Sell': 113.0, '# R.Sell': 64.0, '% R.Sell': 0.57}, '2020-03-31': {'Sell Return%': 3.24, 'Buy Return%': 2.47, '# T.Buy': 116.0, '# R.Buy': 70.0, '% R.Buy': 0.6, '# T.Sell': 133.0, '# R.Sell': 83.0, '% R.Sell': 0.62}, '2020-03-30': {'Sell Return%': -0.24, 'Buy Return%': 2.73, '# T.Buy': 126.0, '# R.Buy': 74.0, '% R.Buy': 0.59, '# T.Sell': 96.0, '# R.Sell': 47.0, '% R.Sell': 0.49}, '2020-03-27': {'Sell Return%': 4.35, 'Buy Return%': 3.28, '# T.Buy': 124.0, '# R.Buy': 75.0, '% R.Buy': 0.6, '# T.Sell': 119.0, '# R.Sell': 72.0, '% R.Sell': 0.61}, '2020-03-26': {'Sell Return%': 1.47, 'Buy Return%': 3.4, '# T.Buy': 127.0, '# R.Buy': 73.0, '% R.Buy': 0.57, '# T.Sell': 114.0, '# R.Sell': 60.0, '% R.Sell': 0.53}, '2020-03-25': {'Sell Return%': 7.75, 'Buy Return%': 7.39, '# T.Buy': 136.0, '# R.Buy': 92.0, '% R.Buy': 0.68, '# T.Sell': 131.0, '# R.Sell': 85.0, '% R.Sell': 0.65}, '2020-03-24': {'Sell Return%': 4.95, 'Buy Return%': 6.99, '# T.Buy': 149.0, '# R.Buy': 95.0, '% R.Buy': 0.64, '# T.Sell': 129.0, '# R.Sell': 77.0, '% R.Sell': 0.6}, '2020-03-23': {'Sell Return%': 8.69, 'Buy Return%': 6.8, '# T.Buy': 138.0, '# R.Buy': 78.0, '% R.Buy': 0.57, '# T.Sell': 151.0, '# R.Sell': 90.0, '% R.Sell': 0.6}, '2020-03-20': {'Sell Return%': 7.91, 'Buy Return%': 2.5, '# T.Buy': 117.0, '# R.Buy': 67.0, '% R.Buy': 0.57, '# T.Sell': 138.0, '# R.Sell': 89.0, '% R.Sell': 0.64}, '2020-03-19': {'Sell Return%': 7.12, 'Buy Return%': 7.62, '# T.Buy': 161.0, '# R.Buy': 98.0, '% R.Buy': 0.61, '# T.Sell': 149.0, '# R.Sell': 84.0, '% R.Sell': 0.56}, '2020-03-18': {'Sell Return%': 5.99, 'Buy Return%': 7.98, '# T.Buy': 135.0, '# R.Buy': 80.0, '% R.Buy': 0.59, '# T.Sell': 141.0, '# R.Sell': 88.0, '% R.Sell': 0.62}, '2020-04-13': {'Sell Return%': 1.0, 'Buy Return%': 1.56, '# T.Buy': 110.0, '# R.Buy': 66.0, '% R.Buy': 0.6, '# T.Sell': 93.0, '# R.Sell': 52.0, '% R.Sell': 0.56}, '2020-04-14': {'Sell Return%': 0.28, 'Buy Return%': 1.13, '# T.Buy': 93.0, '# R.Buy': 52.0, '% R.Buy': 0.56, '# T.Sell': 73.0, '# R.Sell': 36.0, '% R.Sell': 0.49}, '2020-04-15': {'Sell Return%': 1.94, 'Buy Return%': 1.52, '# T.Buy': 120.0, '# R.Buy': 70.0, '% R.Buy': 0.58, '# T.Sell': 110.0, '# R.Sell': 70.0, '% R.Sell': 0.64}, '2020-04-16': {'Sell Return%': 2.44, 'Buy Return%': 0.85, '# T.Buy': 116.0, '# R.Buy': 66.0, '% R.Buy': 0.57, '# T.Sell': 106.0, '# R.Sell': 63.0, '% R.Sell': 0.59}, '2020-04-17': {'Sell Return%': 1.04, 'Buy Return%': 0.43, '# T.Buy': 112.0, '# R.Buy': 61.0, '% R.Buy': 0.54, '# T.Sell': 110.0, '# R.Sell': 59.0, '% R.Sell': 0.54}, '2020-04-20': {'Sell Return%': 1.22, 'Buy Return%': 1.43, '# T.Buy': 94.0, '# R.Buy': 54.0, '% R.Buy': 0.57, '# T.Sell': 99.0, '# R.Sell': 53.0, '% R.Sell': 0.54}, '2020-04-21': {'Sell Return%': 3.81, 'Buy Return%': 1.31, '# T.Buy': 102.0, '# R.Buy': 55.0, '% R.Buy': 0.54, '# T.Sell': 125.0, '# R.Sell': 86.0, '% R.Sell': 0.69}, '2020-04-22': {'Sell Return%': 0.73, 'Buy Return%': 0.83, '# T.Buy': 102.0, '# R.Buy': 59.0, '% R.Buy': 0.58, '# T.Sell': 83.0, '# R.Sell': 46.0, '% R.Sell': 0.55}, '2020-04-23': {'Sell Return%': 3.98, 'Buy Return%': 3.19, '# T.Buy': 140.0, '# R.Buy': 87.0, '% R.Buy': 0.62, '# T.Sell': 151.0, '# R.Sell': 90.0, '% R.Sell': 0.6}, '2020-04-24': {'Sell Return%': 0.47, 'Buy Return%': 2.37, '# T.Buy': 135.0, '# R.Buy': 91.0, '% R.Buy': 0.67, '# T.Sell': 109.0, '# R.Sell': 56.0, '% R.Sell': 0.51}, '2020-04-27': {'Sell Return%': 0.11, 'Buy Return%': 0.75, '# T.Buy': 82.0, '# R.Buy': 56.0, '% R.Buy': 0.68, '# T.Sell': 98.0, '# R.Sell': 48.0, '% R.Sell': 0.49}, '2020-04-28': {'Sell Return%': 3.35, 'Buy Return%': 1.26, '# T.Buy': 101.0, '# R.Buy': 64.0, '% R.Buy': 0.63, '# T.Sell': 114.0, '# R.Sell': 76.0, '% R.Sell': 0.67}, '2020-04-29': {'Sell Return%': -0.24, 'Buy Return%': 0.7, '# T.Buy': 91.0, '# R.Buy': 50.0, '% R.Buy': 0.55, '# T.Sell': 78.0, '# R.Sell': 34.0, '% R.Sell': 0.44}, '2020-04-30': {'Sell Return%': 1.29, 'Buy Return%': 1.43, '# T.Buy': 97.0, '# R.Buy': 60.0, '% R.Buy': 0.62, '# T.Sell': 105.0, '# R.Sell': 61.0, '% R.Sell': 0.58}, '2020-05-01': {'Sell Return%': 1.82, 'Buy Return%': -0.73, '# T.Buy': 87.0, '# R.Buy': 40.0, '% R.Buy': 0.46, '# T.Sell': 95.0, '# R.Sell': 55.0, '% R.Sell': 0.58}, '2020-05-04': {'Sell Return%': 0.45, 'Buy Return%': 1.84, '# T.Buy': 91.0, '# R.Buy': 66.0, '% R.Buy': 0.73, '# T.Sell': 67.0, '# R.Sell': 34.0, '% R.Sell': 0.51}, '2020-05-05': {'Sell Return%': 0.88, 'Buy Return%': 0.48, '# T.Buy': 69.0, '# R.Buy': 38.0, '% R.Buy': 0.55, '# T.Sell': 57.0, '# R.Sell': 28.0, '% R.Sell': 0.49}, '2020-05-06': {'Sell Return%': 0.98, 'Buy Return%': 1.2, '# T.Buy': 84.0, '# R.Buy': 51.0, '% R.Buy': 0.61, '# T.Sell': 79.0, '# R.Sell': 50.0, '% R.Sell': 0.63}, '2020-05-07': {'Sell Return%': 0.39, 'Buy Return%': 0.42, '# T.Buy': 66.0, '# R.Buy': 38.0, '% R.Buy': 0.58, '# T.Sell': 66.0, '# R.Sell': 32.0, '% R.Sell': 0.48}, '2020-05-08': {'Sell Return%': -0.15, 'Buy Return%': 0.43, '# T.Buy': 53.0, '# R.Buy': 34.0, '% R.Buy': 0.64, '# T.Sell': 53.0, '# R.Sell': 23.0, '% R.Sell': 0.43}, '2020-05-11': {'Sell Return%': 0.22, 'Buy Return%': 1.03, '# T.Buy': 63.0, '# R.Buy': 40.0, '% R.Buy': 0.63, '# T.Sell': 49.0, '# R.Sell': 21.0, '% R.Sell': 0.43}, '2020-05-12': {'Sell Return%': 2.15, 'Buy Return%': 0.28, '# T.Buy': 64.0, '# R.Buy': 34.0, '% R.Buy': 0.53, '# T.Sell': 76.0, '# R.Sell': 48.0, '% R.Sell': 0.63}, '2020-05-15': {'Sell Return%': 0.75, 'Buy Return%': 1.04, '# T.Buy': 108.0, '# R.Buy': 60.0, '% R.Buy': 0.56, '# T.Sell': 93.0, '# R.Sell': 50.0, '% R.Sell': 0.54}, '2020-05-13': {'Sell Return%': 4.24, 'Buy Return%': 1.57, '# T.Buy': 115.0, '# R.Buy': 67.0, '% R.Buy': 0.58, '# T.Sell': 128.0, '# R.Sell': 88.0, '% R.Sell': 0.69}, '2020-05-14': {'Sell Return%': 2.67, 'Buy Return%': 2.05, '# T.Buy': 105.0, '# R.Buy': 67.0, '% R.Buy': 0.64, '# T.Sell': 100.0, '# R.Sell': 58.0, '% R.Sell': 0.58}, '2020-05-18': {'Sell Return%': 0.35, 'Buy Return%': 0.52, '# T.Buy': 64.0, '# R.Buy': 38.0, '% R.Buy': 0.59, '# T.Sell': 72.0, '# R.Sell': 37.0, '% R.Sell': 0.51}, '2020-05-19': {'Sell Return%': 1.83, 'Buy Return%': 1.05, '# T.Buy': 62.0, '# R.Buy': 38.0, '% R.Buy': 0.61, '# T.Sell': 74.0, '# R.Sell': 43.0, '% R.Sell': 0.58}, '2020-05-20': {'Sell Return%': 0.2, 'Buy Return%': 0.17, '# T.Buy': 62.0, '# R.Buy': 32.0, '% R.Buy': 0.52, '# T.Sell': 47.0, '# R.Sell': 27.0, '% R.Sell': 0.57}, '2020-05-21': {'Sell Return%': 0.65, 'Buy Return%': 0.31, '# T.Buy': 73.0, '# R.Buy': 35.0, '% R.Buy': 0.48, '# T.Sell': 82.0, '# R.Sell': 46.0, '% R.Sell': 0.56}, '2020-05-22': {'Sell Return%': 0.22, 'Buy Return%': 0.2, '# T.Buy': 59.0, '# R.Buy': 36.0, '% R.Buy': 0.61, '# T.Sell': 61.0, '# R.Sell': 37.0, '% R.Sell': 0.61}, '2020-05-26': {'Sell Return%': 1.59, 'Buy Return%': -0.06, '# T.Buy': 54.0, '# R.Buy': 27.0, '% R.Buy': 0.5, '# T.Sell': 79.0, '# R.Sell': 50.0, '% R.Sell': 0.63}, '2020-05-27': {'Sell Return%': 0.97, 'Buy Return%': 1.17, '# T.Buy': 101.0, '# R.Buy': 63.0, '% R.Buy': 0.62, '# T.Sell': 89.0, '# R.Sell': 51.0, '% R.Sell': 0.57}, '2020-05-29': {'Sell Return%': 1.18, 'Buy Return%': 2.0, '# T.Buy': 85.0, '# R.Buy': 47.0, '% R.Buy': 0.55, '# T.Sell': 83.0, '# R.Sell': 49.0, '% R.Sell': 0.59}, '2020-06-01': {'Sell Return%': -0.33, 'Buy Return%': -0.6, '# T.Buy': 53.0, '# R.Buy': 22.0, '% R.Buy': 0.42, '# T.Sell': 47.0, '# R.Sell': 20.0, '% R.Sell': 0.43}, '2020-06-02': {'Sell Return%': 0.69, 'Buy Return%': 0.76, '# T.Buy': 56.0, '# R.Buy': 36.0, '% R.Buy': 0.64, '# T.Sell': 59.0, '# R.Sell': 34.0, '% R.Sell': 0.58}, '2020-06-03': {'Sell Return%': -0.01, 'Buy Return%': 0.11, '# T.Buy': 53.0, '# R.Buy': 31.0, '% R.Buy': 0.58, '# T.Sell': 51.0, '# R.Sell': 26.0, '% R.Sell': 0.51}, '2020-06-04': {'Sell Return%': 0.17, 'Buy Return%': 0.59, '# T.Buy': 61.0, '# R.Buy': 34.0, '% R.Buy': 0.56, '# T.Sell': 70.0, '# R.Sell': 32.0, '% R.Sell': 0.46}, '2020-06-05': {'Sell Return%': -0.26, 'Buy Return%': 0.74, '# T.Buy': 88.0, '# R.Buy': 45.0, '% R.Buy': 0.51, '# T.Sell': 73.0, '# R.Sell': 33.0, '% R.Sell': 0.45}, '2020-06-08': {'Sell Return%': 0.24, 'Buy Return%': 0.53, '# T.Buy': 57.0, '# R.Buy': 33.0, '% R.Buy': 0.58, '# T.Sell': 55.0, '# R.Sell': 34.0, '% R.Sell': 0.62}, '2020-06-09': {'Sell Return%': 0.2, 'Buy Return%': 1.04, '# T.Buy': 67.0, '# R.Buy': 46.0, '% R.Buy': 0.69, '# T.Sell': 65.0, '# R.Sell': 34.0, '% R.Sell': 0.52}, '2020-06-10': {'Sell Return%': 0.1, 'Buy Return%': 1.37, '# T.Buy': 113.0, '# R.Buy': 65.0, '% R.Buy': 0.58, '# T.Sell': 91.0, '# R.Sell': 48.0, '% R.Sell': 0.53}, '2020-06-11': {'Sell Return%': 1.49, 'Buy Return%': -1.12, '# T.Buy': 103.0, '# R.Buy': 54.0, '% R.Buy': 0.52, '# T.Sell': 129.0, '# R.Sell': 77.0, '% R.Sell': 0.6}, '2020-06-16': {'Sell Return%': 0.45, 'Buy Return%': 0.73, '# T.Buy': 102.0, '# R.Buy': 61.0, '% R.Buy': 0.6, '# T.Sell': 93.0, '# R.Sell': 43.0, '% R.Sell': 0.46}, '2020-06-15': {'Sell Return%': 0.4, 'Buy Return%': 1.79, '# T.Buy': 106.0, '# R.Buy': 62.0, '% R.Buy': 0.58, '# T.Sell': 95.0, '# R.Sell': 51.0, '% R.Sell': 0.54}, '2020-06-17': {'Sell Return%': 0.19, 'Buy Return%': 0.13, '# T.Buy': 86.0, '# R.Buy': 41.0, '% R.Buy': 0.48, '# T.Sell': 73.0, '# R.Sell': 32.0, '% R.Sell': 0.44}, '2020-06-18': {'Sell Return%': -0.31, 'Buy Return%': 0.61, '# T.Buy': 74.0, '# R.Buy': 37.0, '% R.Buy': 0.5, '# T.Sell': 58.0, '# R.Sell': 24.0, '% R.Sell': 0.41}, '2020-06-19': {'Sell Return%': 1.36, 'Buy Return%': 0.65, '# T.Buy': 82.0, '# R.Buy': 49.0, '% R.Buy': 0.6, '# T.Sell': 85.0, '# R.Sell': 53.0, '% R.Sell': 0.62}}
    print(PNLOverDates)
    allData = {}
    allData['PNLOverDates'] = json.dumps(PNLOverDates)
    return allData


############################################
# Live Arbitrage All ETFs
############################################

from MongoDB.PerMinDataOperations import PerMinDataOperations


@app.route('/ETfLiveArbitrage/AllTickers')
def SendLiveArbitrageDataAllTickers():
    try:
        live_data = PerMinDataOperations().LiveFetchPerMinArbitrage()
        live_data.rename(columns={'symbol': 'Symbol'},inplace=True)
        live_prices = PerMinDataOperations().LiveFetchETFPrice()
        ndf = live_data.merge(live_prices, how='left', on='Symbol')
        ndf.dropna(inplace=True)
        ndf=ndf.round(4)
        return ndf.to_dict()
    except Exception as e:
        print("Issue in Flask app while fetching ETF Description Data")
        print(traceback.format_exc())
        return str(e)


############################################
# Live Arbitrage Single ETF
############################################
import time
from FlaskAPI.Components.LiveCalculations.helperLiveArbitrageSingleETF import fecthArbitrageANDLivePrices, analyzeSignalPerformane, AnalyzeDaysPerformance, CategorizeSignals


@app.route('/ETfLiveArbitrage/Single/<etfname>')
def SendLiveArbitrageDataSingleTicker(etfname):
    PerMinObj = PerMinDataOperations()
    res = fecthArbitrageANDLivePrices(etfname=etfname, FuncETFPrices=PerMinObj.FetchFullDayPricesForETF, FuncArbitrageData=PerMinObj.FetchFullDayPerMinArbitrage)
    res['Prices']=res['Prices'].to_csv(sep='\t', index=False)
    res['pnlstatementforday'] = json.dumps(AnalyzeDaysPerformance(ArbitrageDf=res['Arbitrage'],etfname=etfname))
    res['SignalCategorization'] = json.dumps(CategorizeSignals(ArbitrageDf=res['Arbitrage']))
    res['scatterPlotData'] = json.dumps(res['Arbitrage'][['ETF Change Price %','Net Asset Value Change%']].to_dict(orient='records'))
    res['Arbitrage'] = res['Arbitrage'].to_json()
    return json.dumps(res)


@app.route('/ETfLiveArbitrage/Single/UpdateTable/<etfname>')
def UpdateLiveArbitrageDataTablesAndPrices(etfname):
    PerMinObj = PerMinDataOperations()
    res = fecthArbitrageANDLivePrices(etfname=etfname, FuncETFPrices=PerMinObj.LiveFetchETFPrice, FuncArbitrageData=PerMinObj.LiveFetchPerMinArbitrage)
    
    print(res['Arbitrage'])
    res['Prices']=res['Prices'].to_dict()
    res['Arbitrage']=res['Arbitrage'].to_dict()
    res['SignalInfo']=analyzeSignalPerformane(res['Arbitrage']['Arbitrage'][0])
    return res


if __name__ == '__main__':
    app.run(port=5000, debug=True)
