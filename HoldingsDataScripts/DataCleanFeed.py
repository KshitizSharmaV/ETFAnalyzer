import os
import pathlib
import getpass
import traceback
import pandas as pd
from datetime import datetime
from mongoengine.errors import NotUniqueError

from CommonServices.LogCreater import CreateLogger
from CommonServices.EmailService import EmailSender
from MongoDB.Schemas import etfholdings_collection

logger = CreateLogger().createLogFile(dirName='Logs/HoldingsScraperLogs/', logFileName="-HoldingsDataLogs.log",
                                      loggerName='HoldingsLogger')


class PullandCleanData:

    def __init__(self):
        self.rootpath = pathlib.Path(os.getcwd())
        while str(self.rootpath).split('/')[-1] != 'ETFAnalyzer':
            self.rootpath = self.rootpath.parent
        self.savingpath = os.path.abspath(os.path.join(self.rootpath, 'ETFDailyData' + '/' + datetime.now().strftime("%Y%m%d")))
        self.system_username = getpass.getuser()
        self.coll = etfholdings_collection

    def readfilesandclean(self, etfname, etfdescdf):
        try:
            # Trying Dict Comprehension for file checking and loading to check etf file in directory
            x = {f.split('-')[0]: f for f in os.listdir(self.savingpath)}
            if etfname in x.keys():
                print("Data loaded to save into Db = " + etfname)
                logger.debug("Data loaded to save into Db = {}".format(etfname))

                # Read the CSV file, filter the first eleven rows seperated by ":" using regex
                primary_details_df = pd.read_csv(self.savingpath + '/' + x[etfname], sep='\:\s', nrows=11,
                                                 index_col=False,
                                                 names=['Key', 'Value'], engine='python')

                # Read the holdings data from the CSV into DataFrame and Clean the data
                self.holdingsdata = pd.read_csv(self.savingpath + '/' + x[etfname], header=12,
                                                names=['Holdings', 'Symbol', 'Weights'])
                self.holdingsdata['Symbol'] = self.holdingsdata['Symbol'].apply(lambda x: x.split(' ')[0])
                self.holdingsdata['Weights'] = list(map(lambda x: x[:-1], self.holdingsdata['Weights'].values))
                self.holdingsdata['Weights'] = [float(x) for x in self.holdingsdata['Weights'].values]

                ########################################################################################################

                '''ETF Primary Details'''
                primary_details_df = primary_details_df.set_index('Key').T
                primary_details_df.rename(
                    columns={'Inception Date': 'InceptionDate', 'Fund Holdings as of': 'FundHoldingsDate',
                             'Total Assets Under Management (in thousands)': 'TotalAssetsUnderMgmt',
                             'Shares Outstanding': 'SharesOutstanding',
                             'Expense Ratio': 'ExpenseRatio', 'Tracks This Index': 'IndexTracker',
                             'ETFdb.com Category': 'ETFdbCategory', 'Issuer': 'Issuer',
                             'Structure': 'Structure', 'ETF Home Page': 'ETFhomepage'}, inplace=True)
                primary_details_df['ETFTicker'] = etfname
                primary_details_df = primary_details_df[
                    ['ETFTicker', 'InceptionDate', 'FundHoldingsDate', 'TotalAssetsUnderMgmt', 'SharesOutstanding',
                     'ExpenseRatio', 'IndexTracker', 'ETFdbCategory', 'Issuer', 'Structure', 'ETFhomepage']]
                for col in ['TotalAssetsUnderMgmt', 'SharesOutstanding']:
                    primary_details_df[col] = primary_details_df[col].apply(lambda y: int(y))
                primary_details_df['ExpenseRatio'] = primary_details_df['ExpenseRatio'].apply(lambda y: float(y[:-1]))
                primary_details_df['InceptionDate'] = primary_details_df['InceptionDate'].apply(
                    lambda y: datetime.strptime(y, '%Y-%m-%d'))
                primary_details_df['FundHoldingsDate'] = primary_details_df['FundHoldingsDate'].apply(
                    lambda y: datetime.strptime(y, '%Y-%m-%d'))

                ''' ETF Additional Details'''
                additional_details_df = etfdescdf.rename(
                    columns={'ETF Name': 'ETFName', 'Avg. Daily Volume': 'AverageVolume', 'Inverse': 'Inversed',
                             'Leveraged': 'Leveraged', 'Commission Free': 'CommissionFree',
                             'Annual Dividend Rate': 'AnnualDividendRate', 'Dividend Date': 'DividendDate',
                             'Dividend': 'Dividend', 'Annual Dividend Yield %': 'AnnualDividendYield',
                             'P/E Ratio': 'PERatio', 'Beta': 'Beta', '# of Holdings': 'NumberOfHolding',
                             'Liquidity Rating': 'LiquidityRating',
                             'Expenses Rating': 'ExpensesRating', 'Returns Rating': 'ReturnsRating',
                             'Volatility Rating': 'VolatilityRating', 'Dividend Rating': 'DividendRating',
                             'Concentration Rating': 'ConcentrationRating', 'ESG Score': 'ESGScore'})
                additional_details_df.set_index('Symbol', inplace=True)
                additional_details_df = additional_details_df.loc[etfname][
                    ['ETFName', 'AverageVolume', 'Leveraged', 'Inversed', 'CommissionFree', 'AnnualDividendRate',
                     'DividendDate',
                     'Dividend', 'AnnualDividendYield', 'PERatio', 'Beta', 'NumberOfHolding', 'LiquidityRating',
                     'ExpensesRating', 'ReturnsRating',
                     'VolatilityRating', 'DividendRating', 'ConcentrationRating', 'ESGScore']]
                for col in ['CommissionFree', 'PERatio', 'Beta']:
                    additional_details_df[col] = str(additional_details_df[col])

                self.holdingsdata.rename(
                    columns={'Holdings': 'TickerName', 'Symbol': 'TickerSymbol', 'Weights': 'TickerWeight'},
                    inplace=True)
                holdings_data = self.holdingsdata.to_dict(orient='records')

                all_data = {'DateOfScraping': datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)}
                for item in [primary_details_df.to_dict(orient='records')[0],
                             additional_details_df.to_dict(), {'holdings': holdings_data}]:
                    all_data.update(item)
                ########################################################################################################

                print(all_data)
                self.coll.insert(all_data)
                print("Data for {} saved".format(etfname))
                logger.debug("Data for {} saved".format(etfname))
        except FileNotFoundError:
            logger.error("Today's File/Folder Not Found...")
            pass
        except NotUniqueError as NUE:
            logger.critical("Duplicate Entry Error/ Not Unique Error")
            logger.exception(NUE)
            pass
        except Exception as e:
            logger.exception(e)
            logger.critical("Exception occurred in DataCleanFeed.py")
            traceback.print_exc()
            emailobj = EmailSender()
            msg = emailobj.message(subject=e,
                                   text="Exception Caught in ETFAnalysis/HoldingsDataScripts/DataCleanFeed.py {}".format(
                                       traceback.format_exc()))
            emailobj.send(msg=msg, receivers=['piyush888@gmail.com', 'kshitizsharmav@gmail.com'])
