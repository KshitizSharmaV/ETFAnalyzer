from mongoengine import *
from datetime import datetime
import pandas as pd
import getpass
# import logging
#
# log = logging.getLogger()
# log.setLevel(logging.DEBUG)
# logging.basicConfig(filename="LoadEtfs.log", format='%(asctime)s %(message)s', filemode='w')
import logging
import os
path = os.path.join(os.getcwd(), "Logs/")
if not os.path.exists(path):
    os.makedirs(path)

filename = path + datetime.now().strftime("%Y%m%d") + "-ArbEventLog.log"
filename2 = path + datetime.now().strftime("%Y%m%d") + "-ArbErrorLog.log"
handler = logging.FileHandler(filename)
handler2 = logging.FileHandler(filename2)
logging.basicConfig(filename=filename, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', filemode='w')
# logger = logging.getLogger("EventLogger")
logger = logging.getLogger(__name__)
logger2 = logging.getLogger("ArbErrorLogger")
logger.setLevel(logging.DEBUG)
logger2.setLevel(logging.ERROR)
logger.addHandler(handler)
logger2.addHandler(handler2)

from HoldingsDataScripts.ETFMongo import ETF


# from MongoDB.Schemas import connection

class LoadHoldingsdata(object):
    def __init__(self):
        self.cashvalueweight = None
        self.weights = None
        self.symbols = None

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
            logger.debug("Data Successfully Loaded")
            return self
        except Exception as e:
            logger.error("Data Not Loaded")
            # logger.critical(e, exc_info=True)
            logger2.error("Data Not Loaded")
            logger.exception(e)
            logger2.exception(e)

    def getHoldingsDatafromDB(self, etfname, fundholdingsdate):
        try:
            # Production username = ubuntu
            if getpass.getuser() == 'ubuntu':
                # Connect to localhost server for Production
                # connect to 'ETF_db' database in Mongodb with replica set
                # connect('ETF_db', alias='ETF_db', replicaSet='rs0')
                # connect to 'ETF_db' database in Mongodb
                connect('ETF_db', alias='ETF_db')
            else:
                # Connecting to ETF_db on AWS EC2 Production Server
                connect('ETF_db', alias='ETF_db', host='18.213.229.80', port=27017)
            etfdata = ETF.objects(ETFTicker=etfname, FundHoldingsDate__lte=fundholdingsdate).order_by(
                '-FundHoldingsDate').first()
            print(etfdata)
            holdingsdatadf = pd.DataFrame(etfdata.to_mongo().to_dict()['holdings'])
            print(str(etfdata.FundHoldingsDate))
            disconnect('ETF_db')
            return holdingsdatadf

        except Exception as e:
            print("Can't Fetch Fund Holdings Data")
            print(e)
            logger.exception(e)
            logger2.exception(e)
            # logger.critical(e, exc_info=True)
            disconnect('ETF_db')

    def getAllETFData(self, etfname, fundholdingsdate):
        try:
            etfdata = ETF.objects(ETFTicker=etfname, FundHoldingsDate__lte=fundholdingsdate).order_by(
                '-FundHoldingsDate').first()
            return etfdata

        except Exception as e:
            print("Can't Fetch Fund Holdings Data")
            print(e)
            logger.exception(e)
            logger2.exception(e)
            # logger.critical(e, exc_info=True)
            disconnect('ETF_db')

    def getHoldingsDataForAllETFfromDB(self, etfname):
        try:
            # Production username = ubuntu
            if getpass.getuser() == 'ubuntu':
                # Connect to localhost server for Production
                # connect to 'ETF_db' database in Mongodb with replica set
                # connect('ETF_db', alias='ETF_db', replicaSet='rs0')
                # connect to 'ETF_db' database in Mongodb
                connect('ETF_db', alias='ETF_db')
            else:
                # Connecting to ETF_db on AWS EC2 Production Server
                connect('ETF_db', alias='ETF_db', host='18.213.229.80', port=27017)
            etfdata = ETF.objects(ETFTicker=etfname).order_by('-FundHoldingsDate').first()
            print(etfdata.ETFTicker)
            holdingsdatadf = pd.DataFrame(etfdata.to_mongo().to_dict()['holdings'])
            print(str(etfdata.FundHoldingsDate))
            disconnect('ETF_db')
            return holdingsdatadf['TickerSymbol'].to_list()
        except Exception as e:
            print("Can't Fetch Fund Holdings Data for all ETFs")
            print(e)
            logger.exception(e)
            logger2.exception(e)
            disconnect('ETF_db')

    def getETFWeights(self):
        return self.weights

    def getCashValue(self):
        return self.cashvalueweight

    def getSymbols(self):
        return self.symbols
