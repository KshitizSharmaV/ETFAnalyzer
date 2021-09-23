# Add paths to System PATH for the packages to be locatable by python
import pathlib
import sys, traceback

sys.path.append('..')
from time import perf_counter
from datetime import datetime
import pandas as pd
import os
# Use absolute import paths
from CommonServices.LogCreater import CreateLogger
from HoldingsDataScripts.Download523TickersList import Download523TickersList
from HoldingsDataScripts.DownloadHoldings import DownloadsEtfHoldingsData
from HoldingsDataScripts.DataCleanFeed import PullandCleanData
from CommonServices.EmailService import EmailSender
from CommonServices.DirectoryRemover import Directory_Remover
from CommonServices.MultiProcessingTasks import multi_processing_method

t1_start = perf_counter()
logger = CreateLogger().createLogFile(dirName='HoldingsScraperLogs/', logFileName="-HoldingsDataLogs.log",
                                      loggerName='HoldingsLogger')


class holdingsProcess():
    def __init__(self):
        self.rootpath = pathlib.Path(os.getcwd())
        while str(self.rootpath).split('/')[-1] != 'ETFAnalyzer':
            self.rootpath = self.rootpath.parent

        self.csv_file_path = os.path.abspath(os.path.join(self.rootpath, 'CSVFiles/250M_WorkingETFs.csv'))
        self.ticker_description_path = os.path.abspath(os.path.join(self.rootpath,
                                                                    'ETFDailyData/ETFTickersDescription/' + datetime.now().strftime(
                                                                        "%Y%m%d") + '/etfs_details_type_fund_flow.csv'))
        self.url_list = ['https://etfdb.com/etfs/sector/technology/', 'https://etfdb.com/etfs/sector/healthcare/',
                         'https://etfdb.com/etfs/sector/real-estate/', 'https://etfdb.com/etfs/sector/materials/',
                         'https://etfdb.com/etfs/sector/financials/',
                         'https://etfdb.com/etfs/sector/consumer-discretionaries/',
                         'https://etfdb.com/etfs/sector/energy/', 'https://etfdb.com/etfs/sector/telecom/',
                         'https://etfdb.com/etfs/sector/consumer-staples/', 'https://etfdb.com/etfs/sector/utilities/',
                         'https://etfdb.com/etfs/sector/industrials/', 'https://etfdb.com/etfs/country/us/']
        self.list_of_ETFs = pd.read_csv(self.csv_file_path).columns.to_list()
        self.final_ETF_list_DF = pd.DataFrame()

    def get_etf_lists(self):
        for url in self.url_list:
            logger.debug(url)
            Download523TickersList().fetchTickerDataDescription(url)
            df = pd.read_csv(self.ticker_description_path)
            df = df.loc[df['Symbol'].isin(self.list_of_ETFs)]
            os.remove(self.ticker_description_path)
            print("old file removed")
            self.final_ETF_list_DF = pd.concat([self.final_ETF_list_DF, df], ignore_index=True).drop_duplicates(
                subset='Symbol')
        print(self.final_ETF_list_DF)

    def download_and_store_etfs(self, etf):
        print("Processing for {} etf".format(etf))
        logger.debug("Processing for {} etf".format(etf))
        try:
            flag_record = DownloadsEtfHoldingsData().fetchHoldingsofETF(etf)
            if not flag_record:
                PullandCleanData().readfilesandclean(etf, self.final_ETF_list_DF)
        except FileNotFoundError:
            logger.error("Today's File/Folder Not Found...")
            pass
        except Exception as e:
            logger.exception(e)
            traceback.print_exc()
            pass

    def csv_files_clean_up(self):
        status = Directory_Remover(os.path.join(self.rootpath, 'ETFDailyData')).remdir()
        if status:
            print('Successfully deleted downloaded holdings and ticker description CSVs.')
            logger.debug('Successfully deleted downloaded holdings and ticker description CSVs.')
        else:
            print('Problem in deletion of downloaded holdings and ticker description CSVs.')
            logger.error('Problem in deletion of downloaded holdings and ticker description CSVs.')

    def holdings_processes_caller(self):
        self.get_etf_lists()
        multi_processing_method(self.download_and_store_etfs, self.list_of_ETFs)
        self.csv_files_clean_up()


if __name__ == '__main__':
    try:
        holdingsProcess().holdings_processes_caller()
        t1_stop = perf_counter()
        logger.debug("Execution Time (NE) {}".format(t1_stop - t1_start))
        print("Execution Time (NE) {}".format(t1_stop - t1_start))
    except Exception as e:
        print(e)
        logger.exception(e)
        t1_stop = perf_counter()
        logger.debug("Execution Time (E) {} seconds".format(t1_stop - t1_start))
        print("Execution Time (NE) {} seconds".format(t1_stop - t1_start))
        traceback.print_exc()
        # email_obj = EmailSender()
        # msg = email_obj.message(subject=e, text="Exception Caught in ETFAnalysis/HoldingsProcessCaller.py {}".format(
        #     traceback.format_exc()))
        # email_obj.send(msg=msg, receivers=['piyush888@gmail.com', 'kshitizsharmav@gmail.com'])
        pass
