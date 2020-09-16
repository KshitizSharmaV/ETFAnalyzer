import sys
import time

sys.path.append("..")

from flask import jsonify, render_template, Response, g
from flask_cors import CORS
from mongoengine import *
import json
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import traceback
from FlaskAPI.Helpers.CustomAPIErrorHandle import MultipleExceptionHandler, CustomAPIErrorHandler
from MongoDB.MongoDBConnections import MongoDBConnectors
from FlaskAPI.Helpers.FlaskAppMaker import flaskAppMaker
from FlaskAPI.Helpers.APIAuthentication import authAPI
from FlaskAPI.Components.ETFDescription.helper import fetchETFsWithSameIssuer, fetchETFsWithSameETFdbCategory, \
    fetchETFsWithSimilarTotAsstUndMgmt, fetchOHLCHistoricalData
from CalculateETFArbitrage.Helpers.LoadEtfHoldings import LoadHoldingsdata
from FlaskAPI.Components.ETFArbitrage.ETFArbitrageMain import RetrieveETFArbitrageData, retrievePNLForAllDays, \
    OverBoughtBalancedOverSold, CandlesignalsColumns
from FlaskAPI.Components.ETFArbitrage.helperForETFArbitrage import etfMoversChangers
from MongoDB.PerMinDataOperations import PerMinDataOperations
from FlaskAPI.Components.LiveCalculations.helperLiveArbitrageSingleETF import fecthArbitrageANDLivePrices, \
    analyzeSignalPerformane, CategorizeSignals
from CommonServices.Holidays import LastWorkingDay, HolidayCheck
from FlaskAPI.Helpers.ServerLogHelper import custom_server_logger


server_logger = custom_server_logger

connection = MongoDBConnectors().get_pymongo_readonly_devlocal_production()

app = flaskAppMaker().create_app()

CORS(app)

api_auth_object = authAPI()

@app.before_request
def before_request():
    g.start = time.time()

# if sys.platform.startswith('linux') and getpass.getuser() == 'ubuntu':
#     flaskAppMaker().get_index_page()
@app.route('/')
def index():
    return render_template("index.html")


############################################
# ETF Description Page
############################################


@app.route('/api/ETfDescription/getETFWithSameIssuer/<IssuerName>')
def getETFWithSameIssuer(IssuerName):
    res = api_auth_object.authenticate_api()
    if type(res) == Response:
        return res
    try:
        etfswithsameIssuer = fetchETFsWithSameIssuer(
            connection, Issuer=IssuerName)
        if len(etfswithsameIssuer) == 0:
            etfswithsameIssuer['None'] = {'ETFName': 'None',
                                          'TotalAssetsUnderMgmt': "No Other ETF was found with same Issuer"}
        return jsonify(etfswithsameIssuer)
    except Exception as e:
        exc_type, exc_value, exc_tb = sys.exc_info()
        traceback.print_exc()
        return MultipleExceptionHandler().handle_exception(exception_type=exc_type, e=e, custom_logger=server_logger)


@app.route('/api/ETfDescription/getETFsWithSameETFdbCategory/<ETFdbCategory>')
def getETFsWithSameETFdbCategory(ETFdbCategory):
    res = api_auth_object.authenticate_api()
    if type(res) == Response:
        return res
    try:

        etfsWithSameEtfDbCategory = fetchETFsWithSameETFdbCategory(
            connection=connection, ETFdbCategory=ETFdbCategory)
        if len(etfsWithSameEtfDbCategory) == 0:
            etfsWithSameEtfDbCategory['None'] = {'ETFName': 'None',
                                                 'TotalAssetsUnderMgmt': "No Other ETF was found with same ETF DB Category"}
        return jsonify(etfsWithSameEtfDbCategory)
    except Exception as e:
        exc_type, exc_value, exc_tb = sys.exc_info()
        traceback.print_exc()
        return MultipleExceptionHandler().handle_exception(exception_type=exc_type, e=e, custom_logger=server_logger)


@app.route('/api/ETfDescription/getOHLCDailyData/<ETFName>/<StartDate>')
def fetchOHLCDailyData(ETFName, StartDate):
    res = api_auth_object.authenticate_api()
    if type(res) == Response:
        return res
    try:

        StartDate = StartDate.split(' ')[0]
        OHLCData = fetchOHLCHistoricalData(
            etfname=ETFName, StartDate=StartDate)
        OHLCData = OHLCData.to_csv(sep='\t', index=False)
        return OHLCData
    except Exception as e:
        exc_type, exc_value, exc_tb = sys.exc_info()
        traceback.print_exc()
        return MultipleExceptionHandler().handle_exception(exception_type=exc_type, e=e, custom_logger=server_logger)


@app.route('/api/ETfDescription/getHoldingsData/<ETFName>/<StartDate>')
def fetchHoldingsData(ETFName, StartDate):
    res = api_auth_object.authenticate_api()
    if type(res) == Response:
        return res
    try:
        etfdata = LoadHoldingsdata().getAllETFData(ETFName, StartDate)
        if type(etfdata) == Response:
            return etfdata
        ETFDataObject = list(etfdata)[0]
        # HoldingsDatObject=pd.DataFrame(ETFDataObject['holdings']).set_index('TickerSymbol').round(2).T.to_dict()
        # print(HoldingsDatObject)
        return jsonify(ETFDataObject['holdings'])
    except Exception as e:
        exc_type, exc_value, exc_tb = sys.exc_info()
        traceback.print_exc()
        return MultipleExceptionHandler().handle_exception(exception_type=exc_type, e=e, custom_logger=server_logger)


@app.route('/api/ETfDescription/EtfData/<ETFName>/<date>')
def SendETFHoldingsData(ETFName, date):
    res = api_auth_object.authenticate_api()
    if type(res) == Response:
        return res
    try:
        allData = {}
        etfdata = LoadHoldingsdata().getAllETFData(ETFName, date)
        if type(etfdata) == Response:
            return etfdata
        ETFDataObject = list(etfdata)[0]

        allData['SimilarTotalAsstUndMgmt'] = fetchETFsWithSimilarTotAsstUndMgmt(connection=connection,
                                                                                totalassetUnderManagement=ETFDataObject[
                                                                                    'TotalAssetsUnderMgmt'])

        ETFDataObject['TotalAssetsUnderMgmt'] = "${:,.3f} M".format(
            ETFDataObject['TotalAssetsUnderMgmt'] / 1000)
        ETFDataObject['SharesOutstanding'] = "{:,.0f}".format(
            ETFDataObject['SharesOutstanding'])
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
        return MultipleExceptionHandler().handle_exception(exception_type=exc_type, e=e, custom_logger=server_logger)


############################################
# Historical Arbitrage
############################################


# Check if data is avilable only after June 5
def checkifDateIsBeforeJuneFive(datestr):
    date_time_obj = datetime.strptime(datestr, '%Y%m%d')
    return datetime(2020, 6, 5) > date_time_obj


# Divide Columnt into movers and the price by which they are moving
@app.route('/api/PastArbitrageData/<ETFName>/<date>')
def FetchPastArbitrageData(ETFName, date):
    res = api_auth_object.authenticate_api()
    if type(res) == Response:
        return res

    if checkifDateIsBeforeJuneFive(date):
        return CustomAPIErrorHandler().handle_error(
            'Data not available before June 5th 2020, please choose a date after 5th June', 500)
    try:
        ColumnsForDisplay = ['Time', '$Spread', 'Arbitrage in $', 'Absolute Arbitrage',
                             'Over Bought/Sold',
                             'ETFMover%1_ticker',
                             'Change%1_ticker',
                             'T', 'ETF Price']
        # Retreive data for Components
        data, pricedf, PNLStatementForTheDay, scatterPlotData = RetrieveETFArbitrageData(etfname=ETFName, date=date,
                                                                                         magnitudeOfArbitrageToFilterOn=0)
        data = data.sort_index(ascending=False)
        data.index = data.index.time
        data['Time'] = data.index
        pricedf['Time'] = pricedf['date']
        pricedf['Time'] = [x.time() for x in pricedf['Time']]
        pricedf = pd.merge(data[['Time', 'Over Bought/Sold']],
                           pricedf, on='Time', how='right')
        pricedf = pricedf[pricedf['Over Bought/Sold'].notna()]
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

        allData['SignalCategorization'] = CategorizeSignals(ArbitrageDf=data, ArbitrageColumnName='Arbitrage in $',
                                                            PriceColumn='T',
                                                            Pct_change=False)

        data = data.reset_index(drop=True)

        allData['etfhistoricaldata'] = data.to_dict('records')
        allData['ArbitrageCumSum'] = data[::-
        1][['Arbitrage in $', 'Time']].to_dict('records')
        allData['etfPrices'] = pricedf[::-1].to_csv(sep='\t', index=False)
        allData['PNLStatementForTheDay'] = PNLStatementForTheDay
        allData['scatterPlotData'] = scatterPlotData
        allData['etfmoversDictCount'] = etfmoversDictCount
        allData['highestChangeDictCount'] = highestChangeDictCount
        return jsonify(allData)
    except Exception as e:
        exc_type, exc_value, exc_tb = sys.exc_info()
        traceback.print_exc()
        return MultipleExceptionHandler().handle_exception(exception_type=exc_type, e=e, custom_logger=server_logger)


@app.route('/api/PastArbitrageData/CommonDataAcrossEtf/<ETFName>')
def fetchPNLForETFForALlDays(ETFName):
    res = api_auth_object.authenticate_api()
    if type(res) == Response:
        return res

    try:
        PNLOverDates = retrievePNLForAllDays(
            etfname=ETFName, magnitudeOfArbitrageToFilterOn=0)
        PNLOverDates = pd.DataFrame(PNLOverDates).T
        PNLOverDates['Date'] = PNLOverDates.index
        PNLOverDates.columns = ['Sell Return%', 'Buy Return%', '# T.Buy', '# R.Buy', '% R.Buy', '# T.Sell', '# R.Sell',
                                '% R.Sell',
                                'Magnitue Of Arbitrage', 'Date']
        PNLOverDates = PNLOverDates.dropna()
        PNLOverDates = PNLOverDates.to_dict(orient='records')
        return jsonify(PNLOverDates)
    except Exception as e:
        exc_type, exc_value, exc_tb = sys.exc_info()
        traceback.print_exc()
        return MultipleExceptionHandler().handle_exception(exception_type=exc_type, e=e, custom_logger=server_logger)


@app.route('/api/PastArbitrageData/DailyChange/<ETFName>/<date>')
def getDailyChangeUnderlyingStocks(ETFName, date):
    res = api_auth_object.authenticate_api()
    if type(res) == Response:
        return res
    if checkifDateIsBeforeJuneFive(date):
        return CustomAPIErrorHandler().handle_error(
            'Data only available before June 5th 2020, please choose a date after 5th June', 500)
    try:

        etfdata = LoadHoldingsdata().getAllETFData(ETFName, date)
        if type(etfdata) == Response:
            return etfdata
        ETFDataObject = list(etfdata)[0]
        TickerSymbol = pd.DataFrame(ETFDataObject['holdings'])[
            'TickerSymbol'].to_list()
        TickerSymbol.remove('CASH') if 'CASH' in TickerSymbol else TickerSymbol
        openclosedata_cursor = connection.ETF_db.DailyOpenCloseCollection.find(
            {'dateForData': datetime.strptime(date, '%Y%m%d'), 'Symbol': {'$in': TickerSymbol}}, {'_id': 0})
        responses = list(openclosedata_cursor)
        responses = pd.DataFrame.from_records(responses)
        responses['DailyChangepct'] = (
                                              (responses['Close'] - responses['Open Price']) / responses[
                                          'Open Price']) * 100
        responses['DailyChangepct'] = responses['DailyChangepct'].round(3)
        responses.rename(columns={'Symbol': 'symbol',
                                  'Volume': 'volume'}, inplace=True)
        return jsonify(responses[['symbol', 'DailyChangepct', 'volume']].to_dict(orient='records'))
    except Exception as e:
        exc_type, exc_value, exc_tb = sys.exc_info()
        traceback.print_exc()
        return MultipleExceptionHandler().handle_exception(exception_type=exc_type, e=e, custom_logger=server_logger)


############################################
# Live Arbitrage All ETFs
############################################


@app.route('/api/ETfLiveArbitrage/AllTickers')
def SendLiveArbitrageDataAllTickers():
    res = api_auth_object.authenticate_api()
    if type(res) == Response:
        return res
    try:
        live_data = PerMinDataOperations().LiveFetchPerMinArbitrage()
        live_data = live_data[['symbol', 'Arbitrage in $', 'ETF Trading Spread in $', 'ETF Price', 'ETF Change Price %',
                               'Net Asset Value Change%', 'ETFMover%1', 'ETFMover%2', 'ETFMover%3', 'ETFMover%4',
                               'ETFMover%5', 'Change%1', 'Change%2', 'Change%3', 'Change%4', 'Change%5', 'Timestamp']]
        live_data = OverBoughtBalancedOverSold(df=live_data)
        live_data.rename(
            columns={'Magnitude of Arbitrage': 'Absolute Arbitrage'}, inplace=True)

        live_data = live_data.round(3)
        live_data = live_data.fillna(0)
        return jsonify(live_data.to_dict(orient='records'))
    except Exception as e:
        exc_type, exc_value, exc_tb = sys.exc_info()
        traceback.print_exc()
        return MultipleExceptionHandler().handle_exception(exception_type=exc_type, e=e, custom_logger=server_logger)


############################################
# Live Arbitrage Single ETF
############################################
from FlaskAPI.Components.ETFArbitrage.CandleStickResults import AnalyzeCandlestickSignals
import itertools
import functools

analyzeSignalObj = AnalyzeCandlestickSignals()

'''Static unauthorised APIs for XLK Live'''
@functools.lru_cache(maxsize=512)
@app.route('/api/ETfLiveArbitrage/SingleXLKdefault')
def send_live_arbitrage_data_xlk_only():
    try:
        return SendLiveArbitrageDataSingleTicker(etfname='XLK', bypass_auth=True)
    except Exception as e:
        exc_type, exc_value, exc_tb = sys.exc_info()
        traceback.print_exc()
        return MultipleExceptionHandler().handle_exception(exception_type=exc_type, e=e)


@app.route('/api/ETfLiveArbitrage/Single/UpdateTableXLKdefault')
def update_live_arbitrage_data_tables_and_prices_xlk_only():
    try:
        return UpdateLiveArbitrageDataTablesAndPrices(etfname='XLK', bypass_auth=True)
    except Exception as e:
        exc_type, exc_value, exc_tb = sys.exc_info()
        traceback.print_exc()
        return MultipleExceptionHandler().handle_exception(exception_type=exc_type, e=e)


@app.route('/api/ETfLiveArbitrage/Single/SignalAndCandleXLKdefault')
def live_arb_candlestick_and_signal_categorization_xlk_only():
    try:
        return live_arb_candlestick_and_signal_categorization(etfname='XLK', bypass_auth=True)
    except Exception as e:
        exc_type, exc_value, exc_tb = sys.exc_info()
        traceback.print_exc()
        return MultipleExceptionHandler().handle_exception(exception_type=exc_type, e=e)


'''Dynamic APIs for Live Arbitrage'''
'''API for Alpha Candle Stick Pattern Signals Table and Arbitrage Spread Table'''
@app.route('/api/ETfLiveArbitrage/Single/SignalAndCandle/<etfname>')
def live_arb_candlestick_and_signal_categorization(etfname, bypass_auth=False):
    if not bypass_auth:
        res = api_auth_object.authenticate_api()
        if type(res) == Response:
            return res
    else:
        pass
    try:
        PerMinObj = PerMinDataOperations()
        res = fecthArbitrageANDLivePrices(etfname=etfname, FuncETFPrices=PerMinObj.FetchFullDayPricesForETF,
                                          FuncArbitrageData=PerMinObj.FetchFullDayPerMinArbitrage,
                                          callAllDayArbitrage=True)
        if type(res) == Response:
            return res
        last_minute = res['Arbitrage']['Time'].tail(1).values[0]
        signals_dict = analyzeSignalObj.analyze_etf_for_all_patterns(res['Arbitrage'])
        list(map(
            lambda x: signals_dict.update({x: ('No Occurrence yet', 'No Signal')}) if x not in signals_dict else None,
            CandlesignalsColumns))
        signals_dict = [[x.replace(' ', ''), *v] for x, v in signals_dict.items()]
        last_minute_signal = list(itertools.filterfalse(lambda x: last_minute not in x, signals_dict))
        last_minute_signal = " ".join(last_minute_signal[0]) if len(last_minute_signal) > 0 else "No Pattern"

        res['SignalCategorization'] = CategorizeSignals(ArbitrageDf=res['Arbitrage'],
                                                        ArbitrageColumnName='Arbitrage in $', PriceColumn='ETF Price',
                                                        Pct_change=True)
        res['CandlestickSignals'] = signals_dict
        res['last_minute_signal'] = last_minute_signal
        res.pop('Prices')
        res.pop('Arbitrage')
        return jsonify(res)
    except Exception as e:
        exc_type, exc_value, exc_tb = sys.exc_info()
        traceback.print_exc()
        return MultipleExceptionHandler().handle_exception(exception_type=exc_type, e=e, custom_logger=server_logger)

'''Static unauthorised APIs for XLK Live'''
@functools.lru_cache(maxsize=512)
@app.route('/api/ETfLiveArbitrage/SingleXLKdefault')
def send_live_arbitrage_data_xlk_only():
    try:
        return SendLiveArbitrageDataSingleTicker(etfname='XLK', bypass_auth=True)
    except Exception as e:
        exc_type, exc_value, exc_tb = sys.exc_info()
        traceback.print_exc()
        return MultipleExceptionHandler().handle_exception(exception_type=exc_type, e=e)


@app.route('/api/ETfLiveArbitrage/Single/UpdateTableXLKdefault')
def update_live_arbitrage_data_tables_and_prices_xlk_only():
    try:
        return UpdateLiveArbitrageDataTablesAndPrices(etfname='XLK', bypass_auth=True)
    except Exception as e:
        exc_type, exc_value, exc_tb = sys.exc_info()
        traceback.print_exc()
        return MultipleExceptionHandler().handle_exception(exception_type=exc_type, e=e)


@app.route('/api/ETfLiveArbitrage/Single/SignalAndCandleXLKdefault')
def live_arb_candlestick_and_signal_categorization_xlk_only():
    try:
        return live_arb_candlestick_and_signal_categorization(etfname='XLK', bypass_auth=True)
    except Exception as e:
        exc_type, exc_value, exc_tb = sys.exc_info()
        traceback.print_exc()
        return MultipleExceptionHandler().handle_exception(exception_type=exc_type, e=e)


'''Dynamic APIs for Live Arbitrage'''
'''API for Alpha Candle Stick Pattern Signals Table and Arbitrage Spread Table'''
@app.route('/api/ETfLiveArbitrage/Single/SignalAndCandle/<etfname>')
def live_arb_candlestick_and_signal_categorization(etfname, bypass_auth=False):
    if not bypass_auth:
        res = api_auth_object.authenticate_api()
        if type(res) == Response:
            return res
    else:
        pass
    try:
        PerMinObj = PerMinDataOperations()
        res = fecthArbitrageANDLivePrices(etfname=etfname, FuncETFPrices=PerMinObj.FetchFullDayPricesForETF,
                                          FuncArbitrageData=PerMinObj.FetchFullDayPerMinArbitrage,
                                          callAllDayArbitrage=True)
        if type(res) == Response:
            return res
        last_minute = res['Arbitrage']['Time'].tail(1).values[0]
        signals_dict = analyzeSignalObj.analyze_etf_for_all_patterns(res['Arbitrage'])
        list(map(
            lambda x: signals_dict.update({x: ('No Occurrence yet', 'No Signal')}) if x not in signals_dict else None,
            CandlesignalsColumns))
        signals_dict = [[x.replace(' ', ''), *v] for x, v in signals_dict.items()]
        last_minute_signal = list(itertools.filterfalse(lambda x: last_minute not in x, signals_dict))
        last_minute_signal = " ".join(last_minute_signal[0]) if len(last_minute_signal) > 0 else "No Pattern"
        pricedf = res['Prices']
        pricedf = pricedf.reset_index(drop=True)
        pricedf['Time'] = pricedf['date']
        pricedf['Time'] = pricedf['Time'].apply(lambda x: str(x.time()))
        pricedf = pd.merge(res['Arbitrage'][['Time', 'Over Bought/Sold']], pricedf, on='Time', how='right')
        pricedf = pricedf[pricedf['Over Bought/Sold'].notna()]
        del pricedf['Time']
        res['Prices'] = pricedf
        res['Prices'] = res['Prices'].to_csv(sep='\t', index=False)
        res['SignalCategorization'] = CategorizeSignals(ArbitrageDf=res['Arbitrage'],
                                                        ArbitrageColumnName='Arbitrage in $', PriceColumn='ETF Price',
                                                        Pct_change=True)
        res['CandlestickSignals'] = signals_dict
        res['last_minute_signal'] = last_minute_signal
        res.pop('Arbitrage')
        return jsonify(res)
    except Exception as e:
        exc_type, exc_value, exc_tb = sys.exc_info()
        traceback.print_exc()
        return MultipleExceptionHandler().handle_exception(exception_type=exc_type, e=e)


@app.route('/api/ETfLiveArbitrage/Single/<etfname>')
def SendLiveArbitrageDataSingleTicker(etfname, bypass_auth=False):
    if not bypass_auth:
        res = api_auth_object.authenticate_api()
        if type(res) == Response:
            return res
    else:
        pass
    try:
        PerMinObj = PerMinDataOperations()
        res = fecthArbitrageANDLivePrices(etfname=etfname, FuncETFPrices=PerMinObj.FetchFullDayPricesForETF,
                                          FuncArbitrageData=PerMinObj.FetchFullDayPerMinArbitrage,
                                          callAllDayArbitrage=True)
        if type(res) == Response:
            return res
        last_minute = res['Arbitrage']['Time'].tail(1).values[0]
        signals_dict = analyzeSignalObj.analyze_etf_for_all_patterns(res['Arbitrage'])
        list(map(
            lambda x: signals_dict.update({x: ('No Occurrence yet', 'No Signal')}) if x not in signals_dict else None,
            CandlesignalsColumns))
        signals_dict = [[x.replace(' ', ''), *v] for x, v in signals_dict.items()]
        last_minute_signal = list(itertools.filterfalse(lambda x: last_minute not in x, signals_dict))
        last_minute_signal = " ".join(last_minute_signal[0]) if len(last_minute_signal) > 0 else "No Pattern"
        pricedf = res['Prices']
        pricedf = pricedf.reset_index(drop=True)
        pricedf['Time'] = pricedf['date']
        pricedf['Time'] = pricedf['Time'].apply(lambda x: str(x.time()))
        pricedf = pd.merge(res['Arbitrage'][['Time', 'Over Bought/Sold']], pricedf, on='Time', how='right')
        pricedf = pricedf[pricedf['Over Bought/Sold'].notna()]
        del pricedf['Time']
        res['Prices'] = pricedf
        res['Prices'] = res['Prices'].to_csv(sep='\t', index=False)
        res['pnlstatementforday'] = res['pnlstatementforday']
        res['SignalCategorization'] = CategorizeSignals(ArbitrageDf=res['Arbitrage'],
                                                        ArbitrageColumnName='Arbitrage in $', PriceColumn='ETF Price',
                                                        Pct_change=True)
        etfmoversDictCount, highestChangeDictCount = etfMoversChangers(res['Arbitrage'])
        res['etfmoversDictCount'] = etfmoversDictCount
        res['highestChangeDictCount'] = highestChangeDictCount
        res['scatterPlotData'] = res['Arbitrage'][['ETF Change Price %', 'Net Asset Value Change%']].to_dict(
            orient='records')
        res['ArbitrageLineChart'] = res['Arbitrage'][['Arbitrage in $', 'Time']].to_dict('records')
        res['SignalInfo'] = analyzeSignalPerformane(res['Arbitrage'].tail(1).to_dict('records')[0]['Arbitrage in $'])
        arbitrage_columns = list(res['Arbitrage'].columns)
        res['Arbitrage'].rename(columns={x: x.replace(' ', '_') for x in arbitrage_columns}, inplace=True)
        res['Arbitrage'] = res['Arbitrage'].fillna(0)
        res['Arbitrage'] = res['Arbitrage'].to_dict(orient='records')
        res['CandlestickSignals'] = signals_dict
        res['last_minute_signal'] = last_minute_signal
        return jsonify(res)
    except Exception as e:
        exc_type, exc_value, exc_tb = sys.exc_info()
        traceback.print_exc()
        return MultipleExceptionHandler().handle_exception(exception_type=exc_type, e=e, custom_logger=server_logger)


@app.route('/api/ETfLiveArbitrage/Single/UpdateTable/<etfname>')
def UpdateLiveArbitrageDataTablesAndPrices(etfname, bypass_auth=False):
    if not bypass_auth:
        res = api_auth_object.authenticate_api()
        if type(res) == Response:
            return res
    else:
        pass
    try:
        PerMinObj = PerMinDataOperations()
        res = fecthArbitrageANDLivePrices(etfname=etfname, FuncETFPrices=PerMinObj.LiveFetchETFPrice,
                                          FuncArbitrageData=PerMinObj.LiveFetchPerMinArbitrage,
                                          callAllDayArbitrage=False)
        if type(res) == Response:
            return res
        etfmoversDictCount, highestChangeDictCount = etfMoversChangers(res['Arbitrage'])
        res['etfmoversDictCount'] = etfmoversDictCount
        res['highestChangeDictCount'] = highestChangeDictCount
        res['Arbitrage'] = OverBoughtBalancedOverSold(df=res['Arbitrage'])
        arbitrage_columns = list(res['Arbitrage'].columns)
        res['Arbitrage'].rename(columns={x: x.replace(' ', '_') for x in arbitrage_columns if ' ' in x}, inplace=True)
        res['Prices'] = res['Prices'].to_dict(orient='records')[0]
        res['Arbitrage'] = res['Arbitrage'].to_dict(orient='records')[0]
        res['SignalInfo'] = analyzeSignalPerformane(
            res['Arbitrage']['Arbitrage_in_$'])
        return res
    except Exception as e:
        exc_type, exc_value, exc_tb = sys.exc_info()
        traceback.print_exc()
        return MultipleExceptionHandler().handle_exception(exception_type=exc_type, e=e, custom_logger=server_logger)


############################################
# Get last working date
############################################


@app.route('/api/LastWorkingDate/')
def LastWorkingDate():
    lastworkinDay = LastWorkingDay(
        datetime.utcnow().date() - timedelta(days=2))
    return json.dumps(datetime.strftime(lastworkinDay.date(), '%Y%m%d'))


@app.route('/api/ListOfHolidays')
def ListOfHolidays():
    mydates = pd.date_range(
        '2020-06-05', datetime.today().date().strftime("%Y-%m-%d")).tolist()
    MyholidayList = [date.date().strftime("%Y-%m-%d")
                     for date in mydates if HolidayCheck(date)]

    return jsonify({'HolidayList': MyholidayList})

@app.after_request
def after_request(response):
    diff = time.time() - g.start
    server_logger.debug(f"Total server side exec time: {diff}")
    return response


if __name__ == '__main__':
    app.run(port=5000, debug=True)
