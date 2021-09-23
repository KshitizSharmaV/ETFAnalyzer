import getpass
import os
import pathlib
import time
import traceback
import sys  # Remove in production - KTZ

sys.path.append("../..")  # Remove in production - KTZ
from CommonServices.Holidays import HolidayCheck
import pandas as pd
from datetime import datetime, timedelta
from CalculateETFArbitrage.Helpers.LoadEtfHoldings import LoadHoldingsdata
from MongoDB.MongoDBConnections import MongoDBConnectors
from CalculateETFArbitrage.CalculateHistoricalArbitrage import ArbitrageCalculation
from MongoDB.SaveArbitrageCalcs import SaveCalculatedArbitrage
from MongoDB.FetchArbitrage import FetchArbitrage
from CommonServices.EmailService import EmailSender
from CommonServices.LogCreater import CreateLogger
from CommonServices.MakeCSV import CSV_Maker

logger = CreateLogger().createLogFile(dirName="HistoricalArbitrage/", logFileName="-ArbEventLog.log",
                                      loggerName="RepairHistArbEventLogger",
                                      filemode='a')
logger2 = CreateLogger().createLogFile(dirName="HistoricalArbitrage/", logFileName="-ArbErrorLog.log",
                                       loggerName="RepairHistArbErrorLogger",
                                       filemode='a')


class HistoricalArbitrageDataRepairClass():
    """For repair task, change Dates and ETF List in the 'all_task_runner' method"""

    def __init__(self):
        self.etflist = []
        self.etfwhichfailed = []
        self.date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        self.rootpath = pathlib.Path(os.getcwd())
        while str(self.rootpath).split('/')[-1] != 'ETFAnalyzer':
            self.rootpath = self.rootpath.parent

        self.csv_file_path = os.path.abspath(os.path.join(self.rootpath, 'CSVFiles/250M_WorkingETFs.csv'))

    def get_updated_etf_list(self):
        # MAKE A LIST OF WORKING ETFs.
        workingdf = pd.read_csv(self.csv_file_path)
        workinglist = workingdf.columns.to_list()
        workinglist = ['IJR', 'XAR', 'XLB', 'PTH', 'USSG', 'FXG', 'IVW', 'VBK', 'FTA', 'SUSL', 'RYU', 'USMV', 'XMLV',
                       'QLD', 'PSJ', 'PSCH', 'FUTY', 'IVV', 'XME', 'XLV', 'SUSA', 'FIDU', 'RWL', 'FHLC', 'IGM', 'IWP',
                       'IYM', 'RYH', 'XLK', 'QQQ', 'VOOV', 'IJH', 'QTEC', 'GSLC', 'IVOG', 'VOT', 'IYT', 'SPLV', 'SSO',
                       'IJS', 'SPSM', 'RSP', 'IYC', 'MTUM', 'FDIS', 'FXL', 'FIVG', 'IAT', 'VOE', 'SPLG', 'OEF', 'PHO',
                       'IGV', 'IYE', 'FBT', 'IYJ', 'MDYG', 'KIE', 'RPV', 'SOXL', 'VTV', 'XSLV', 'PEY', 'PWV', 'VOOG',
                       'XNTK', 'XBI', 'FREL', 'XRT', 'IYF', 'KRE', 'FDN', 'IJJ', 'SLYV', 'SPYD', 'XLI', 'FTEC', 'RWR',
                       'FTCS', 'SKYY', 'SMH', 'VUG', 'XLY', 'SPXU', 'RHS', 'BBRE', 'FSTA', 'SCHV', 'VGT', 'DLN', 'XLC',
                       'XMMO', 'FEX', 'SPYG', 'XSD', 'XLF', 'SCHG', 'ESGU', 'VPU', 'PNQI', 'XHB', 'SPY', 'LABU', 'PPA',
                       'IYK', 'MGK', 'PBW', 'REZ', 'IYW', 'IDU', 'IYZ', 'BBH', 'SLY', 'FXU', 'SCHH', 'KBWB', 'PSI',
                       'SOXX', 'IUSV', 'IYH', 'FIW', 'IVE', 'SPYV', 'VLUE', 'IHF', 'AMLP', 'FENY', 'VNQ', 'FXH', 'RYT',
                       'VO', 'VV', 'IJK', 'NOBL', 'IGE', 'KBE', 'IBB', 'VOO', 'PJP', 'IYR', 'ICF', 'REM', 'NAIL',
                       'SLYG', 'MGC', 'TQQQ', 'XLE', 'XLP', 'SRVR', 'SCHX', 'ITB', 'PSCT', 'ITA', 'TDIV', 'VHT', 'VCR',
                       'VIOO', 'PWB', 'XLU', 'VDE', 'VAW', 'FNX', 'TECL', 'XOP', 'IJT', 'PEJ', 'IUSG', 'DIA', 'IWS',
                       'VDC', 'XHE', 'ROM', 'VIS']
        print("List of working ETFs:")
        print(workinglist)
        print(len(workinglist))

        # CHECK ARBITRAGE COLLECTION FOR ETFs ALREADY PRESENT.
        arb_db_data = FetchArbitrage().fetch_arbitrage_data(self.date)
        arb_db_data_etflist = [arbdata['ETFName'] for arbdata in arb_db_data]
        arb_db_data_etflist = list(set(arb_db_data_etflist))
        print("List of ETFs whose arbitrage calculation is present in DB:")
        print(arb_db_data_etflist)
        print(len(arb_db_data_etflist))

        # REMOVE THE ETFs, FROM WORKING ETF LIST, WHOSE ARBITRAGE HAS ALREADY BEEN CALCULATED.
        print("Updated etflist:")
        workingset = set(workinglist)
        doneset = set(arb_db_data_etflist)
        self.etflist = list(workingset.difference(doneset))
        print(self.etflist)
        print(len(self.etflist))

    def remove_old_trades_quotes_for_the_date_etf(self):
        """REMOVE ALL QUOTES DATA AND TRADES DATA FOR THE UPDATED ETF LIST FOR THE GIVEN DATE"""
        try:
            del_list = self.etflist.copy()
            if getpass.getuser() == 'ubuntu':
                rem_conn = MongoDBConnectors().get_pymongo_readWrite_production_production()
            else:
                rem_conn = MongoDBConnectors().get_pymongo_devlocal_devlocal()
            quotes_del = rem_conn.ETF_db.QuotesData.delete_many(
                {'dateForData': datetime.strptime(self.date, '%Y-%m-%d'), 'symbol': {'$in': del_list}})
            print(quotes_del.deleted_count)
            sym_list = [del_list.extend(
                LoadHoldingsdata().LoadHoldingsAndClean(etf, datetime.strptime(self.date, '%Y-%m-%d')).getSymbols()) for
                etf
                in self.etflist]
            trades_del = rem_conn.ETF_db.TradesData.delete_many(
                {'dateForData': datetime.strptime(self.date, '%Y-%m-%d'), 'symbol': {'$in': del_list}})
            print(trades_del.deleted_count)
        except Exception as e:
            logger.exception(e)
            logger2.exception(e)
            pass

    def format_and_save_data(self, etfname, data):
        try:
            data.reset_index(inplace=True)
            data['ETFName'] = etfname
            data['dateOfAnalysis'] = datetime.strptime(self.date, '%Y-%m-%d')
            data['dateWhenAnalysisRan'] = datetime.now()
            SaveCalculatedArbitrage().insertIntoCollection(ETFName=etfname, data=data.to_dict(orient='records'))
        except Exception as e:
            traceback.print_exc()
            logger.exception(e)

    def calculate_arbitrage_for_etf(self, etfname, date):
        try:
            print("Doing Analysis for ETF= " + etfname)
            logger.debug("Doing Analysis for ETF= {}".format(etfname))
            data = ArbitrageCalculation().calculateArbitrage(etfname, date)

            if data is None:
                print("Holding Belong to some other Exchange, No data was found")
                logger.error("Holding Belong to some other Exchange, No data was found for {}".format(etfname))
                self.etfwhichfailed.append(etfname)
                return
            else:
                self.format_and_save_data(etfname, data)
        except Exception as e:
            self.etfwhichfailed.append(etfname)
            print("exception in {} etf, not crawled".format(etfname))
            print(e)
            traceback.print_exc()
            logger.warning("exception in {} etf, not crawled".format(etfname))
            logger.exception(e)
            logger2.warning("exception in {} etf, not crawled".format(etfname))
            logger2.exception(e)
            # emailobj = EmailSender()
            # msg = emailobj.message(subject=e,
            #                        text="Exception Caught in ETFAnalysis/CalculateETFArbitrage/HistoricalArbCaller.py for etf: {} \n {}".format(
            #                            etfname,
            #                            traceback.format_exc()))
            # emailobj.send(msg=msg, receivers=['piyush888@gmail.com', 'kshitizsharmav@gmail.com'])
            return

    def write_etfwhichfailed_tocsv(self):
        if len(self.etfwhichfailed) > 0:
            CSV_Maker().write_to_csv(self.etfwhichfailed, "etfwhichfailed.csv")

    def all_task_runner(self):
        dates = ['2021-09-07', '2021-09-08', '2021-09-09', '2021-09-10', '2021-09-13', '2021-09-14']
        for date_ in dates:
            if HolidayCheck(datetime.strptime(date_, '%Y-%m-%d').date()):
                logger.info("Holiday. Moving to next date...")
                continue
            self.date = date_
            self.get_updated_etf_list()
            self.etflist = ['SPY', 'VOO', 'QQQ', 'IVV', 'IJR', 'VO', 'VGT', 'XLK', 'XLF', 'SCHX']
            # self.etflist = ['IJR']
            self.remove_old_trades_quotes_for_the_date_etf()
            for etf in self.etflist:
                start = time.time()
                self.calculate_arbitrage_for_etf(etf, self.date)
                print(f"Time Taken: {time.time() - start} seconds")
            self.write_etfwhichfailed_tocsv()
            print(self.etflist)


if __name__ == '__main__':
    HistoricalArbitrageDataRepairClass().all_task_runner()
