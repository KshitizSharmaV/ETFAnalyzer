import sys
sys.path.append("..")
from pymongo.errors import ConnectionFailure,ServerSelectionTimeoutError, NetworkTimeout
from flask import Flask, request, jsonify, Response
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
from FlaskAPI.Helpers.CustomAPIErrorHandle import MultipleExceptionHandler, CustomAPIErrorHandler
import time

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


@app.route('/api/ETfDescription/getETFWithSameIssuer/<IssuerName>')
def getETFWithSameIssuer(IssuerName):
    try:
        etfswithsameIssuer = fetchETFsWithSameIssuer(connection, Issuer=IssuerName)
        if len(etfswithsameIssuer) == 0:
            etfswithsameIssuer['None'] = {'ETFName': 'None',
                                          'TotalAssetsUnderMgmt': "No Other ETF was found with same Issuer"}
        return jsonify(etfswithsameIssuer)
    except Exception as e:
        exc_type, exc_value, exc_tb = sys.exc_info()
        traceback.print_exc()
        return MultipleExceptionHandler().handle_exception(exception_type=exc_type, e=e)


@app.route('/api/ETfDescription/getETFsWithSameETFdbCategory/<ETFdbCategory>')
def getETFsWithSameETFdbCategory(ETFdbCategory):
    try:
        etfsWithSameEtfDbCategory = fetchETFsWithSameETFdbCategory(connection=connection, ETFdbCategory=ETFdbCategory)
        if len(etfsWithSameEtfDbCategory) == 0:
            etfsWithSameEtfDbCategory['None'] = {'ETFName': 'None',
                                                 'TotalAssetsUnderMgmt': "No Other ETF was found with same ETF DB Category"}
        return jsonify(etfsWithSameEtfDbCategory)
    except Exception as e:
        exc_type, exc_value, exc_tb = sys.exc_info()
        traceback.print_exc()
        return MultipleExceptionHandler().handle_exception(exception_type=exc_type, e=e)


@app.route('/api/ETfDescription/getOHLCDailyData/<ETFName>/<StartDate>')
def fetchOHLCDailyData(ETFName, StartDate):
    try:
        StartDate = StartDate.split(' ')[0]
        OHLCData = fetchOHLCHistoricalData(etfname=ETFName, StartDate=StartDate)
        OHLCData = OHLCData.to_csv(sep='\t', index=False)
        return OHLCData
    except Exception as e:
        exc_type, exc_value, exc_tb = sys.exc_info()
        traceback.print_exc()
        return MultipleExceptionHandler().handle_exception(exception_type=exc_type, e=e)


@app.route('/api/ETfDescription/getHoldingsData/<ETFName>/<StartDate>')
def fetchHoldingsData(ETFName, StartDate):
    try:
        print("StartDate:{}".format(StartDate))
        MongoDBConnectors().get_mongoengine_readonly_devlocal_production()
        etfdata = LoadHoldingsdata().getAllETFData(ETFName, StartDate)
        if type(etfdata) == Response:
            return etfdata
        ETFDataObject = etfdata.to_mongo().to_dict()
        # HoldingsDatObject=pd.DataFrame(ETFDataObject['holdings']).set_index('TickerSymbol').round(2).T.to_dict()
        # print(HoldingsDatObject)
        return jsonify(ETFDataObject['holdings'])
    except Exception as e:
        exc_type, exc_value, exc_tb = sys.exc_info()
        traceback.print_exc()
        return MultipleExceptionHandler().handle_exception(exception_type=exc_type, e=e)

@app.route('/api/ETfDescription/EtfData/<ETFName>/<date>')
def SendETFHoldingsData(ETFName, date):
    try:
        allData = {}
        MongoDBConnectors().get_mongoengine_readonly_devlocal_production()
        etfdata = LoadHoldingsdata().getAllETFData(ETFName, date)
        if type(etfdata) == Response:
            return etfdata
        ETFDataObject = etfdata.to_mongo().to_dict()

        allData['SimilarTotalAsstUndMgmt'] = fetchETFsWithSimilarTotAsstUndMgmt(connection=connection,
                                                                                totalassetUnderManagement=ETFDataObject[
                                                                                    'TotalAssetsUnderMgmt'])

        ETFDataObject['TotalAssetsUnderMgmt'] = "${:,.3f} M".format(ETFDataObject['TotalAssetsUnderMgmt'] / 1000)
        ETFDataObject['SharesOutstanding'] = "{:,.0f}".format(ETFDataObject['SharesOutstanding'])
        ETFDataObject['InceptionDate'] = str(ETFDataObject['InceptionDate'])
        # List of columns we don't need
        for v in ['_id', 'DateOfScraping', 'ETFhomepage', 'holdings', 'FundHoldingsDate']:
            del ETFDataObject[v]
        ETFDataObject = pd.DataFrame(ETFDataObject, index=[0])
        ETFDataObject = ETFDataObject.replace(np.nan, 'nan', regex=True)
        ETFDataObject = ETFDataObject.loc[0].to_dict()

        allData['ETFDataObject'] = ETFDataObject
        return json.dumps(allData)
    except Exception as e:
        exc_type, exc_value, exc_tb = sys.exc_info()
        traceback.print_exc()
        return MultipleExceptionHandler().handle_exception(exception_type=exc_type, e=e)


############################################
# Historical Arbitrage
############################################
from FlaskAPI.Components.ETFArbitrage.ETFArbitrageMain import RetrieveETFArbitrageData, retrievePNLForAllDays, OverBoughtBalancedOverSold
from FlaskAPI.Components.ETFArbitrage.helperForETFArbitrage import etfMoversChangers


# Divide Columnt into movers and the price by which they are moving
@app.route('/api/PastArbitrageData/<ETFName>/<date>')
def FetchPastArbitrageData(ETFName, date):
    try:
        print("Historical Data For %s & date %s" %(ETFName,str(date)))
        ColumnsForDisplay = ['Time', '$Spread', 'Arbitrage in $', 'Absolute Arbitrage',
                             'Over Bought/Sold',
                             'ETFMover%1_ticker',
                             'Change%1_ticker',
                             'T', 'T+1']
        # Retreive data for Components
        data, pricedf, PNLStatementForTheDay, scatterPlotData = RetrieveETFArbitrageData(etfname=ETFName, date=date,
                                                                                         magnitudeOfArbitrageToFilterOn=0)
        data = data.sort_index()
        data.index = data.index.time
        data['Time'] = data.index
        pricedf['Time']=pricedf['date']
        pricedf['Time'] = [x.time() for x in pricedf['Time']]
        pricedf=pd.merge(data[['Time','Over Bought/Sold']],pricedf,on='Time',how='right')
        pricedf =pricedf[pricedf['Over Bought/Sold'].notna()]
        del pricedf['Time']

        # Seperate ETF Movers and the Underlying with highest change %
        etfmoversDictCount, highestChangeDictCount = etfMoversChangers(data)

        data.index = data.index.astype(str)

        # Round of DataFrame
        data = data.round(3)

        # Replace Values in Pandas DataFrame
        data.rename(columns={'ETF Trading Spread in $': '$Spread',
                             'Magnitude of Arbitrage': 'Absolute Arbitrage'}, inplace=True)

        # Get the price dataframe
        allData = {}
        data['Time'] = data.index
        data = data[ColumnsForDisplay]
        print("Historical DataFrame")
        print(data)

        allData['SignalCategorization'] = json.dumps(
            CategorizeSignals(ArbitrageDf=data, ArbitrageColumnName='Arbitrage in $', PriceColumn='T', Pct_change=False))

        data = data.reset_index(drop=True)

        allData['etfhistoricaldata'] = data.to_json()
        allData['ArbitrageCumSum'] = data[['Arbitrage in $', 'Time']].to_dict('records')
        allData['etfPrices'] = pricedf.to_csv(sep='\t', index=False)
        allData['PNLStatementForTheDay'] = json.dumps(PNLStatementForTheDay)
        allData['scatterPlotData'] = json.dumps(scatterPlotData)
        allData['etfmoversDictCount'] = json.dumps(etfmoversDictCount)
        allData['highestChangeDictCount'] = json.dumps(highestChangeDictCount)
        return json.dumps(allData)
    except Exception as e:
        exc_type, exc_value, exc_tb = sys.exc_info()
        traceback.print_exc()
        return MultipleExceptionHandler().handle_exception(exception_type=exc_type, e=e)



@app.route('/api/PastArbitrageData/CommonDataAcrossEtf/<ETFName>')
def fetchPNLForETFForALlDays(ETFName):
    try:
        print("All ETF PNL Statement is called")
        PNLOverDates = retrievePNLForAllDays(etfname=ETFName, magnitudeOfArbitrageToFilterOn=0)
        PNLOverDates = pd.DataFrame(PNLOverDates).T
        PNLOverDates['Date'] = PNLOverDates.index
        PNLOverDates.columns = ['Sell Return%', 'Buy Return%', '# T.Buy', '# R.Buy', '% R.Buy', '# T.Sell', '# R.Sell',
                                '% R.Sell',
                                'Magnitue Of Arbitrage', 'Date']
        PNLOverDates = PNLOverDates.to_dict(orient='records')
        print("PNLOverDates: "+str(PNLOverDates))
        return jsonify(PNLOverDates)
    except Exception as e:
        exc_type, exc_value, exc_tb = sys.exc_info()
        traceback.print_exc()
        return MultipleExceptionHandler().handle_exception(exception_type=exc_type, e=e)



from PolygonTickData.PolygonCreateURLS import PolgonDataCreateURLS
from CommonServices.ThreadingRequests import IOBoundThreading
import requests


@app.route('/api/PastArbitrageData/DailyChange/<ETFName>/<date>')
def getDailyChangeUnderlyingStocks(ETFName, date):
    try:
        MongoDBConnectors().get_mongoengine_readonly_devlocal_production()
        etfdata = LoadHoldingsdata().getAllETFData(ETFName, date)
        if type(etfdata) == Response:
            return etfdata
        ETFDataObject = etfdata.to_mongo().to_dict()
        TickerSymbol = pd.DataFrame(ETFDataObject['holdings'])['TickerSymbol'].to_list()
        TickerSymbol.remove('CASH') if 'CASH' in TickerSymbol else TickerSymbol
        openclosedata_cursor = connection.ETF_db.DailyOpenCloseCollection.find(
            {'dateForData': datetime.strptime(date, '%Y%m%d'), 'Symbol': {'$in': TickerSymbol}}, {'_id': 0})
        responses = list(openclosedata_cursor)
        responses = pd.DataFrame.from_records(responses)
        responses['DailyChangepct'] = ((responses['Close'] - responses['Open Price']) / responses['Open Price']) * 100
        responses['DailyChangepct'] = responses['DailyChangepct'].round(3)
        responses.rename(columns={'Symbol': 'symbol', 'Volume': 'volume'}, inplace=True)
        return jsonify(responses[['symbol', 'DailyChangepct', 'volume']].to_dict(orient='records'))
    except Exception as e:
        exc_type, exc_value, exc_tb = sys.exc_info()
        traceback.print_exc()
        return MultipleExceptionHandler().handle_exception(exception_type=exc_type, e=e)



############################################
# Live Arbitrage All ETFs
############################################

from MongoDB.PerMinDataOperations import PerMinDataOperations


@app.route('/api/ETfLiveArbitrage/AllTickers')
def SendLiveArbitrageDataAllTickers():
    try:
        print("All Etfs Live Arbitrage is called")
        live_data = PerMinDataOperations().LiveFetchPerMinArbitrage()
        live_data = live_data[['symbol', 'Arbitrage in $', 'ETF Trading Spread in $', 'ETF Price', 'ETF Change Price %',
                               'Net Asset Value Change%', 'ETFMover%1', 'ETFMover%2', 'ETFMover%3', 'ETFMover%4',
                               'ETFMover%5', 'Change%1', 'Change%2', 'Change%3', 'Change%4', 'Change%5', 'Timestamp']]
        live_data=OverBoughtBalancedOverSold(df=live_data)
        live_data.rename(columns={'Magnitude of Arbitrage': 'Absolute Arbitrage'}, inplace=True)

        live_data = live_data.round(3)
        live_data = live_data.fillna(0)
        print(live_data)
        print(live_data.columns)
        return jsonify(live_data.to_dict(orient='records'))
    except Exception as e:
        exc_type, exc_value, exc_tb = sys.exc_info()
        traceback.print_exc()
        return MultipleExceptionHandler().handle_exception(exception_type=exc_type, e=e)


############################################
# Live Arbitrage Single ETF
############################################
from FlaskAPI.Components.LiveCalculations.helperLiveArbitrageSingleETF import fecthArbitrageANDLivePrices, \
    analyzeSignalPerformane, AnalyzeDaysPerformance, CategorizeSignals


@app.route('/api/ETfLiveArbitrage/Single/<etfname>')
def SendLiveArbitrageDataSingleTicker(etfname):
    try:
        PerMinObj = PerMinDataOperations()
        res = fecthArbitrageANDLivePrices(etfname=etfname, FuncETFPrices=PerMinObj.FetchFullDayPricesForETF,
                                          FuncArbitrageData=PerMinObj.FetchFullDayPerMinArbitrage, callAllDayArbitrage=True)
        if type(res) == Response:
            return res
        pricedf= res['Prices']
        pricedf =pricedf.reset_index(drop=True)
        pricedf['Time']=pricedf['date']
        pricedf['Time'] = pricedf['Time'].apply(lambda x: str(x.time()))
        pricedf=pd.merge(res['Arbitrage'][['Time','Over Bought/Sold']],pricedf,on='Time',how='right')
        pricedf =pricedf[pricedf['Over Bought/Sold'].notna()]
        del pricedf['Time']
        res['Prices'] = pricedf

        res['Prices'] = res['Prices'].to_csv(sep='\t', index=False)
        res['pnlstatementforday'] = json.dumps(res['pnlstatementforday'])
        res['SignalCategorization'] = json.dumps(
            CategorizeSignals(ArbitrageDf=res['Arbitrage'], ArbitrageColumnName='Arbitrage in $', PriceColumn='Price',
                              Pct_change=True))
        print(res['Arbitrage'])

        etfmoversDictCount, highestChangeDictCount = etfMoversChangers(res['Arbitrage'])
        res['etfmoversDictCount'] = json.dumps(etfmoversDictCount)
        res['highestChangeDictCount'] = json.dumps(highestChangeDictCount)

        res['scatterPlotData'] = json.dumps(
            res['Arbitrage'][['ETF Change Price %', 'Net Asset Value Change%']].to_dict(orient='records'))
        res['ArbitrageLineChart'] = res['Arbitrage'][['Arbitrage in $', 'Time']].to_dict('records')
        res['Arbitrage'] = res['Arbitrage'].to_json()
        return json.dumps(res)
    except Exception as e:
        exc_type, exc_value, exc_tb = sys.exc_info()
        traceback.print_exc()
        return MultipleExceptionHandler().handle_exception(exception_type=exc_type, e=e)



@app.route('/api/ETfLiveArbitrage/Single/UpdateTable/<etfname>')
def UpdateLiveArbitrageDataTablesAndPrices(etfname):
    try:
        PerMinObj = PerMinDataOperations()
        res = fecthArbitrageANDLivePrices(etfname=etfname, FuncETFPrices=PerMinObj.LiveFetchETFPrice,
                                          FuncArbitrageData=PerMinObj.LiveFetchPerMinArbitrage, callAllDayArbitrage=False)
        if type(res) == Response:
            return res
        res['Prices'] = res['Prices'].to_dict()
        res['Arbitrage'] = res['Arbitrage'].to_dict()
        res['SignalInfo'] = analyzeSignalPerformane(res['Arbitrage']['Arbitrage in $'][0])
        return res
    except Exception as e:
        exc_type, exc_value, exc_tb = sys.exc_info()
        traceback.print_exc()
        return MultipleExceptionHandler().handle_exception(exception_type=exc_type, e=e)



if __name__ == '__main__':
    app.run(port=5000, debug=True)
