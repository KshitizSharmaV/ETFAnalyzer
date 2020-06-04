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
from datetime import datetime
import traceback
import sys



sys.path.append("..")

app = Flask(__name__)

CORS(app)

# Production Local Server
# connect('ETF_db', alias='ETF_db')
# Production Server
connection = connect('ETF_db', alias='ETF_db', host='18.213.229.80', port=27017)
############################################
# Load ETF Holdings Data and Description
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
# Load Past Arbitrage Past Data
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
# Live Arbitrage Single Page
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
        print(e)
        return str(e)


############################################
# Live Arbitrage Single Page
############################################
import time

@app.route('/ETfLiveArbitrage/Single/<etfname>')
def SendLiveArbitrageDataSingleTicker(etfname):
    start_time = time.time()
    try:
        etf_full_day_price_cursor = PerMinDataOperations().FetchFullDayPricesForETF(etfname)
        etf_full_day_price_data = []
        [etf_full_day_price_data.append(item) for item in etf_full_day_price_cursor]
        full_day_prices_df = pd.DataFrame.from_records(etf_full_day_price_data)
        full_day_prices_df.rename(columns={'sym': 'Symbol', 'vw': 'Price', 'e': 'Timestamp'}, inplace=True)
        full_day_prices_df.drop(columns=['Symbol'], inplace=True)
        print("Price Dataframe")
        print(full_day_prices_df)

        live_data = PerMinDataOperations().FetchPerMinLiveData(etfname=etfname)
        data1 = []
        [data1.append({'Timestamp': item['Timestamp'], 'Symbol': item['ArbitrageData'][0]['Symbol'],
                       'Arbitrage': item['ArbitrageData'][0]['Arbitrage'],
                       'Spread': item['ArbitrageData'][0]['Spread']}) for item in live_data]
        live_data_df = pd.DataFrame.from_records(data1)

        full_day_data = PerMinDataOperations().FetchPerMinArbitrageFullDay(etfname=etfname)
        data = []
        [data.append({'Timestamp': item['Timestamp'], 'Symbol': item['ArbitrageData'][0]['Symbol'],
                      'Arbitrage': item['ArbitrageData'][0]['Arbitrage'], 'Spread': item['ArbitrageData'][0]['Spread']})
         for item in full_day_data]
        full_day_data_df = pd.DataFrame.from_records(data)
        print(full_day_data_df)

        mergedDF = full_day_data_df.merge(full_day_prices_df, on='Timestamp', how='left')
        print("Merged Dataaframe")
        print(mergedDF)
        # live_data_df = mergedDF.loc[mergedDF['Timestamp'].idxmax()]
        # live_data_dict = live_data_df.to_dict()

        # return "Live: {}, Full_Day: {}".format(live_data_df.to_dict(), full_day_data_df.to_dict())
        print("--- %s seconds ---" % (time.time() - start_time))
        return jsonify(Live=live_data_df.to_dict(), Full_Day=mergedDF.to_dict())

    except Exception as e:
        print("Issue in Flask app while fetching ETF Description Data")
        print(e)
        return str(e)

@app.route('/ETfLiveArbitrage/Single/UpdateTable/<etfname>')
def UpdateLiveArbitrageDataTablesAndPrices(etfname):
    start_time = time.time()
    try:
        etf_full_day_price_cursor = PerMinDataOperations().FetchFullDayPricesForETF(etfname)
        etf_full_day_price_data = []
        [etf_full_day_price_data.append(item) for item in etf_full_day_price_cursor]
        full_day_prices_df = pd.DataFrame.from_records(etf_full_day_price_data)
        full_day_prices_df.rename(columns={'sym': 'Symbol', 'vw': 'Price', 'e': 'Timestamp'}, inplace=True)
        full_day_prices_df.drop(columns=['Symbol'], inplace=True)
        print("Price Dataframe")
        print(full_day_prices_df)

        live_data = PerMinDataOperations().FetchPerMinLiveData(etfname=etfname)
        data1 = []
        [data1.append({'Timestamp': item['Timestamp'], 'Symbol': item['ArbitrageData'][0]['Symbol'],
                       'Arbitrage': item['ArbitrageData'][0]['Arbitrage'],
                       'Spread': item['ArbitrageData'][0]['Spread']}) for item in live_data]
        live_data_df = pd.DataFrame.from_records(data1)

        full_day_data = PerMinDataOperations().FetchPerMinArbitrageFullDay(etfname=etfname)
        data = []
        [data.append({'Timestamp': item['Timestamp'], 'Symbol': item['ArbitrageData'][0]['Symbol'],
                      'Arbitrage': item['ArbitrageData'][0]['Arbitrage'], 'Spread': item['ArbitrageData'][0]['Spread']})
         for item in full_day_data]
        full_day_data_df = pd.DataFrame.from_records(data)
        print(full_day_data_df)

        mergedDF = full_day_data_df.merge(full_day_prices_df, on='Timestamp', how='left')
        print("Merged Dataaframe")
        print(mergedDF)
        # live_data_df = mergedDF.loc[mergedDF['Timestamp'].idxmax()]
        # live_data_dict = live_data_df.to_dict()

        # return "Live: {}, Full_Day: {}".format(live_data_df.to_dict(), full_day_data_df.to_dict())
        print("--- %s seconds ---" % (time.time() - start_time))
        return jsonify(Live=live_data_df.to_dict(), Full_Day=mergedDF.to_dict())

    except Exception as e:
        print("Issue in Flask app while fetching ETF Description Data")
        print(e)
        return str(e)


if __name__ == '__main__':
    app.run(port=5000, debug=True)
