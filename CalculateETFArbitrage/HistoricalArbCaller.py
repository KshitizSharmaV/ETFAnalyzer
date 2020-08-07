import os
import pathlib
import sys  # Remove in production - KTZ
import traceback

sys.path.append("..")  # Remove in production - KTZ
from CommonServices.EmailService import EmailSender
from CommonServices.MakeCSV import CSV_Maker
import pandas as pd
from datetime import datetime
from datetime import timedelta
from CalculateETFArbitrage.CalculateHistoricalArbitrage import ArbitrageCalculation
from MongoDB.SaveArbitrageCalcs import SaveCalculatedArbitrage
from MongoDB.FetchArbitrage import FetchArbitrage
from CommonServices.LogCreater import CreateLogger

logger = CreateLogger().createLogFile(dirName="Logs/", logFileName="-ArbEventLog.log", loggerName="ArbEventLogger",
                                      filemode='w')
logger2 = CreateLogger().createLogFile(dirName="Logs/", logFileName="-ArbErrorLog.log", loggerName="ArbErrorLogger",
                                       filemode='w')


class HistoricalArbitrageRunnerClass():
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
        self.get_updated_etf_list()
        for etf in self.etflist:
            self.calculate_arbitrage_for_etf(etf, self.date)
        self.write_etfwhichfailed_tocsv()
        print(self.etflist)
        print(self.etfwhichfailed)

if __name__ == '__main__':
    HistoricalArbitrageRunnerClass().all_task_runner()