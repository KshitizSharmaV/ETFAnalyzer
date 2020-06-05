import getpass
import traceback

import pandas as pd
from datetime import datetime
from mongoengine import *
from mongoengine.errors import NotUniqueError
from HoldingsDataScripts.ETFMongo import ETF
from HoldingsDataScripts.HoldingsMongo import Holdings
from CommonServices.EmailService import EmailSender

import logging
import os

from MongoDB.MongoDBConnections import MongoDBConnectors

path = os.path.join(os.getcwd(), "Logs/HoldingsScraperLogs/")
if not os.path.exists(path):
    os.makedirs(path)
filename = path + datetime.now().strftime("%Y%m%d") + "-HoldingsDataLogs.log"
handler = logging.FileHandler(filename)
logging.basicConfig(filename=filename, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.ERROR)
logger.addHandler(handler)


class PullandCleanData:

    def __init__(self):
        self.savingpath = './ETFDailyData' + '/' + datetime.now().strftime("%Y%m%d")
        self.detailsdata = pd.DataFrame()
        self.holdingsdata = pd.DataFrame()
        self.system_username = getpass.getuser()
        if self.system_username == 'ubuntu':
            ''' Production to Production readWrite '''
            MongoDBConnectors().get_mongoengine_readWrite_production_production()
        else:
            ''' Dev Local to Production Read Only '''
            # MongoDBConnectors().get_mongoengine_readonly_devlocal_production()
            ''' Dev Local to Dev Local readWrite '''
            MongoDBConnectors().get_mongoengine_devlocal_devlocal()

    def readfilesandclean(self, etfname, etfdescdf):
        # take etf name to be stored and respective data in DataFrame format
        try:
            # Trying Dict Comprehension for file checking and loading to check etf file in directory
            x = {f.split('-')[0]: f for f in os.listdir(self.savingpath)}
            if etfname in x.keys():
                print("Data loaded to save into Db = " + etfname)
                logger.debug("Data loaded to save into Db = {}".format(etfname))

                # Read the CSV file, filter the first eleven rows seperated by ":" using regex
                self.detailsdata = pd.read_csv(self.savingpath + '/' + x[etfname], sep='\:\s', nrows=11,
                                               index_col=False,
                                               names=['Key', 'Value'])

                # Clean Data
                # Turn to float
                self.detailsdata.iloc[5]['Value'] = float(self.detailsdata.iloc[5]['Value'][:-1])

                # Read the holdings data from the CSV into DataFrame and Clean the data
                self.holdingsdata = pd.read_csv(self.savingpath + '/' + x[etfname], header=12,
                                                names=['Holdings', 'Symbol', 'Weights'])
                self.holdingsdata['Weights'] = list(map(lambda x: x[:-1], self.holdingsdata['Weights'].values))
                self.holdingsdata['Weights'] = [float(x) for x in self.holdingsdata['Weights'].values]

                # Make an ETF object for each holding to be saved as a document in the collection
                details = ETF(
                    DateOfScraping=datetime.now().date(),
                    ETFTicker=str(self.detailsdata.iloc[0]['Key']),
                    InceptionDate=datetime.strptime(self.detailsdata.iloc[1]['Value'], '%Y-%m-%d'),
                    FundHoldingsDate=datetime.strptime(self.detailsdata.iloc[2]['Value'], '%Y-%m-%d'),
                    TotalAssetsUnderMgmt=int(self.detailsdata.iloc[3]['Value']),
                    SharesOutstanding=int(self.detailsdata.iloc[4]['Value']),
                    ExpenseRatio=str(self.detailsdata.iloc[5]['Value']),
                    IndexTracker=str(self.detailsdata.iloc[6]['Value']),
                    ETFdbCategory=str(self.detailsdata.iloc[7]['Value']),
                    Issuer=str(self.detailsdata.iloc[8]['Value']),
                    Structure=str(self.detailsdata.iloc[9]['Value']),
                    ETFhomepage=str(self.detailsdata.iloc[10]['Value']),
                    ETFName=str(etfdescdf.loc[etfdescdf['Symbol'] == etfname]['ETFName'].values[0]),
                    AverageVolume=str(etfdescdf.loc[etfdescdf['Symbol'] == etfname]['AverageVolume'].values[0]),
                    Leveraged=str(etfdescdf.loc[etfdescdf['Symbol'] == etfname]['Leveraged'].values[0]),
                    Inversed=str(etfdescdf.loc[etfdescdf['Symbol'] == etfname]['Inversed'].values[0]),
                    CommissionFree=str(
                        etfdescdf.loc[etfdescdf['Symbol'] == etfname]['CommissionFree'].values[0]),
                    AnnualDividendRate=str(
                        etfdescdf.loc[etfdescdf['Symbol'] == etfname]['AnnualDividendRate'].values[0]),
                    DividendDate=str(etfdescdf.loc[etfdescdf['Symbol'] == etfname]['DividendDate'].values[0]),
                    Dividend=str(etfdescdf.loc[etfdescdf['Symbol'] == etfname]['Dividend'].values[0]),
                    AnnualDividendYield=str(
                        etfdescdf.loc[etfdescdf['Symbol'] == etfname]['AnnualDividendYield'].values[0]),
                    PERatio=str(etfdescdf.loc[etfdescdf['Symbol'] == etfname]['PERatio'].values[0]),
                    Beta=str(etfdescdf.loc[etfdescdf['Symbol'] == etfname]['Beta'].values[0]),
                    NumberOfHolding=float(etfdescdf.loc[etfdescdf['Symbol'] == etfname]['NumberOfHolding'].values[0]),
                    # OverAllRating=str(etfdescdf.loc[etfdescdf['Symbol'] == etfname]['OverAllRating'].values[0]),
                    LiquidityRating=str(
                        etfdescdf.loc[etfdescdf['Symbol'] == etfname]['LiquidityRating'].values[0]),
                    ExpensesRating=str(
                        etfdescdf.loc[etfdescdf['Symbol'] == etfname]['ExpensesRating'].values[0]),
                    ReturnsRating=str(etfdescdf.loc[etfdescdf['Symbol'] == etfname]['ReturnsRating'].values[0]),
                    VolatilityRating=str(
                        etfdescdf.loc[etfdescdf['Symbol'] == etfname]['VolatilityRating'].values[0]),
                    DividendRating=str(
                        etfdescdf.loc[etfdescdf['Symbol'] == etfname]['DividendRating'].values[0]),
                    ConcentrationRating=str(
                        etfdescdf.loc[etfdescdf['Symbol'] == etfname]['ConcentrationRating'].values[0]),
                    ESGScore=float(etfdescdf.loc[etfdescdf['Symbol'] == etfname]['ESGScore'].values[0]),
                )
                # For the said document for given etf, feed all the holdings from dataframe into embedded field list
                for index, row in self.holdingsdata.iterrows():
                    holding = Holdings()
                    holding.TickerName = str(row.Holdings)
                    holding.TickerSymbol = str(row.Symbol)
                    holding.TickerWeight = row.Weights
                    details.holdings.append(holding)
                details.save()  # save the document into the collection in db
                disconnect('ETF_db')
                print("Data for {} saved".format(etfname))
                logger.debug("Data for {} saved".format(etfname))
        except FileNotFoundError:
            pass
            logger.error("Today's File/Folder Not Found...")
            disconnect('ETF_db')
        except NotUniqueError:
            logger.critical("Duplicate Entry Error/ Not Unique Error")
            logger.exception("Exception occurred in DataCleanFeed.py")
            pass
        except Exception as e:
            logger.critical(e)
            logger.exception("Exception occurred in DataCleanFeed.py")
            traceback.print_exc()
            emailobj = EmailSender()
            msg = emailobj.message(subject="Exception Occurred",
                                   text="Exception Caught in ETFAnalysis/HoldingsDataScripts/DataCleanFeed.py {}".format(
                                       traceback.format_exc()))
            emailobj.send(msg=msg, receivers=['piyush888@gmail.com', 'kshitizsharmav@gmail.com'])
            disconnect('ETF_db')


if __name__ == "__main__":
    PullandCleanData().readfilesandclean()
