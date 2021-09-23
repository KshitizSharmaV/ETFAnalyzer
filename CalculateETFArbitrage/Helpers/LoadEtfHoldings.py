import socket
import sys

sys.path.append('../..')
import traceback

from datetime import datetime
import pandas as pd
import getpass
from CommonServices.LogCreater import CreateLogger
from FlaskAPI.Helpers.CustomAPIErrorHandle import MultipleExceptionHandler

logger = CreateLogger().createLogFile(dirName="HistoricalArbitrage/", logFileName="-ArbEventLog.log",
                                      loggerName="ETFHoldingsAPILogger",
                                      filemode='a')
logger2 = CreateLogger().createLogFile(dirName="HistoricalArbitrage/", logFileName="-ArbErrorLog.log",
                                       loggerName="ETFHoldingsAPIErrorLogger",
                                       filemode='a')

from MongoDB.MongoDBConnections import MongoDBConnectors
from FlaskAPI.Helpers.ServerLogHelper import custom_server_logger


class LoadHoldingsdata(object):
    def __init__(self):
        self.cashvalueweight = None
        self.weights = None
        self.symbols = None
        self.system_username = getpass.getuser()
        sys_private_ip = socket.gethostbyname(socket.gethostname())
        if sys_private_ip == '172.31.76.32' and self.system_username == 'ubuntu':
            self.conn = MongoDBConnectors().get_pymongo_readWrite_production_production()
        else:
            # self.conn = MongoDBConnectors().get_pymongo_readonly_devlocal_production()
            self.conn = MongoDBConnectors().get_pymongo_devlocal_devlocal()

    def LoadHoldingsAndClean(self, etfname, fundholdingsdate):
        try:
            holdings = self.getHoldingsDatafromDB(etfname, fundholdingsdate)
            holdings['TickerWeight'] = holdings['TickerWeight'] / 100
            # Assign cashvalueweight
            try:
                self.cashvalueweight = holdings[holdings['TickerSymbol'] == 'CASH'].get('TickerWeight').item()
            except:
                self.cashvalueweight = 0
                pass

            # Assign Weight %
            self.weights = dict(zip(holdings.TickerSymbol, holdings.TickerWeight))

            # Assign symbols
            symbols = holdings['TickerSymbol'].tolist()
            symbols.append(etfname)
            try:
                symbols.remove('CASH')
            except:
                pass
            self.symbols = symbols
            logger.debug(f"Data Successfully Loaded for {etfname}")
            return self
        except Exception as e:
            logger.error("Holdings Data Not Loaded for etf : {}", format(etfname))
            logger2.error("Holdings Data Not Loaded for etf : {}", format(etfname))
            logger.exception(e)
            logger2.exception(e)

    def get_total_asset_under_mgmt(self, etf_name, fund_holdings_date):
        try:
            if not type(fund_holdings_date) == datetime:
                fund_holdings_date = datetime.strptime(fund_holdings_date, '%Y-%m-%d')
            etf_data = self.conn.ETF_db.ETFHoldings.find(
                {'ETFTicker': etf_name, 'FundHoldingsDate': {'$lte': fund_holdings_date}},
                {'_id': 0, 'TotalAssetsUnderMgmt': 1}).sort(
                [('FundHoldingsDate', -1)]).limit(1)
            taum = list(etf_data)[0]['TotalAssetsUnderMgmt']
            return taum
        except Exception as e:
            logger.error("Holdings Data Not Loaded for etf : {}", format(etf_name))
            logger2.error("Holdings Data Not Loaded for etf : {}", format(etf_name))
            logger.exception(e)
            logger2.exception(e)

    def getHoldingsDatafromDB(self, etfname, fundholdingsdate):
        try:
            if not type(fundholdingsdate) == datetime:
                fundholdingsdate = datetime.strptime(fundholdingsdate, '%Y-%m-%d')
            etfdata = self.conn.ETF_db.ETFHoldings.find(
                {'ETFTicker': etfname, 'FundHoldingsDate': {'$lte': fundholdingsdate}}).sort(
                [('FundHoldingsDate', -1)]).limit(1)
            etfdata = list(etfdata)[0]
            holdingsdatadf = pd.DataFrame(etfdata['holdings'])
            print(str(etfdata['FundHoldingsDate']))
            return holdingsdatadf

        except Exception as e:
            print("Can't Fetch Fund Holdings Data for etf {}".format(etfname))
            logger.error("Can't Fetch Fund Holdings Data for etf {}".format(etfname))
            print(e)
            logger.exception(e)
            logger2.exception(e)
            # logger.critical(e, exc_info=True)

    def getAllETFData(self, etfname, fundholdingsdate):
        try:
            if not type(fundholdingsdate) == datetime:
                fundholdingsdate = datetime.strptime(fundholdingsdate, '%Y%m%d')
            etfdata = self.conn.ETF_db.ETFHoldings.find(
                {'ETFTicker': etfname, 'FundHoldingsDate': {'$lte': fundholdingsdate}}).sort(
                [('FundHoldingsDate', -1)]).limit(1)
            return etfdata

        except Exception as e:
            print("Can't Fetch Fund Holdings Data for etf: {}".format(etfname))
            logger.error("Can't Fetch Fund Holdings Data for etf: {}".format(etfname))
            logger2.error("Can't Fetch Fund Holdings Data for etf: {}".format(etfname))
            print(e)
            traceback.print_exc()
            logger.exception(e)
            logger2.exception(e)
            exc_type, exc_value, exc_tb = sys.exc_info()
            return MultipleExceptionHandler().handle_exception(exception_type=exc_type, e=e,
                                                               custom_logger=custom_server_logger)

    def getHoldingsDataForAllETFfromDB(self, etfname):
        try:
            etfdata = self.conn.ETF_db.ETFHoldings.find({'ETFTicker': etfname}).sort([('FundHoldingsDate', -1)]).limit(
                1)
            etfdata = list(etfdata)[0]
            print(etfdata['ETFTicker'])
            holdingsdatadf = pd.DataFrame(etfdata['holdings'])
            print(str(etfdata['FundHoldingsDate']))
            return holdingsdatadf['TickerSymbol'].to_list()
        except Exception as e:
            print("Can't Fetch Fund Holdings Data for all ETFs")
            logger.error("Can't Fetch Fund Holdings Data for all ETFs")
            logger2.error("Can't Fetch Fund Holdings Data for all ETFs")
            print(e)
            logger.exception(e)
            logger2.exception(e)
            traceback.print_exc()

    def getETFWeights(self):
        return self.weights

    def getCashValue(self):
        return self.cashvalueweight

    def getSymbols(self):
        return self.symbols
