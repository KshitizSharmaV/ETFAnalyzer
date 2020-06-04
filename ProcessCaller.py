# Add paths to System PATH for the packages to be locatable by python
import sys, traceback
from time import perf_counter

t1_start = perf_counter()
# For Piyush System
sys.path.extend(['/home/piyush/Desktop/etf0406', '/home/piyush/Desktop/etf0406/ETFAnalyzer', '/home/piyush/Desktop/etf0406/ETFAnalyzer/ETFsList_Scripts',
                 '/home/piyush/Desktop/etf0406/ETFAnalyzer/HoldingsDataScripts',
                 '/home/piyush/Desktop/etf0406/ETFAnalyzer/CommonServices',
                 '/home/piyush/Desktop/etf0406/ETFAnalyzer/CalculateETFArbitrage'])
# For Production env
sys.path.extend(['/home/ubuntu/ETFAnalyzer', '/home/ubuntu/ETFAnalyzer/ETFsList_Scripts',
                 '/home/ubuntu/ETFAnalyzer/HoldingsDataScripts', '/home/ubuntu/ETFAnalyzer/CommonServices',
                 '/home/ubuntu/ETFAnalyzer/CalculateETFArbitrage'])
sys.path.append("..")  # Remove in production - KTZ
# Use absolute import paths
import CommonServices.WebdriverServices as serv
from ETFsList_Scripts.Download523TickersList import Download523TickersList
from HoldingsDataScripts.DownloadHoldings import DownloadsEtfHoldingsData, PullHoldingsListClass
from HoldingsDataScripts.DataCleanFeed import PullandCleanData
from datetime import datetime
from CommonServices.EmailService import EmailSender
from CommonServices.DirectoryRemover import Directory_Remover
import logging
# Check for system via username of the system
import getpass
import os

path = os.path.join(os.getcwd(), "Logs/HoldingsScraperLogs/")
if not os.path.exists(path):
    os.makedirs(path)
filename = path + datetime.now().strftime("%Y%m%d") + "-HoldingsDataLogs.log"
handler = logging.FileHandler(filename)
logging.basicConfig(filename=filename, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.addHandler(handler)


def startCronJobForETFHoldings():
    Download523TickersList().fetchTickerDataDescription()
    serv.masterclass().savelisttodb()
    # Pull ETF list into a dataframe
    ETFListDF = PullHoldingsListClass().ReturnetflistDF()

    # For each ETF download all holdings and save to DB
    for etf in ETFListDF['Symbol'].tolist():
        # for etf in ['DIET']:
        print("Processing for {} etf".format(etf))
        logger.debug("Processing for {} etf".format(etf))
        try:
            # Download Holdings for given ETF
            # Will also return flag for DataCleanFeed for whether the record is already present
            FlagRecord = DownloadsEtfHoldingsData().fetchHoldingsofETF(etf)

            if not FlagRecord:
                # Save Holdings for given ETF to DB
                PullandCleanData().readfilesandclean(etf, ETFListDF)

        except FileNotFoundError:
            logger.error("Today's File/Folder Not Found...")
            continue
        except Exception as e:
            logger.exception(e)
            continue


try:

    startCronJobForETFHoldings()
    # Begin deletion process after storing to DB is finished
    # Delete both 523 ETF List CSV file and Downloaded Ticker CSV files
    username = getpass.getuser()
    if username == 'piyush':
        Directory_Remover('/home/piyush/Desktop/etf1903/ETFDailyData').remdir()
    else:
        Directory_Remover('/home/ubuntu/ETFAnalysis/ETFDailyData').remdir()
    t1_stop = perf_counter()
    logger.debug("Execution Time (NE) {}".format(t1_stop - t1_start))
    print("Execution Time (NE) {}".format(t1_stop - t1_start))
except Exception as e:
    print(e)
    logger.exception("Exception in ProcessCaller")
    t1_stop = perf_counter()
    logger.debug("Execution Time (E) {}".format(t1_stop - t1_start))
    print("Execution Time (NE) {}".format(t1_stop - t1_start))
    # receivers' address in a list (1 or more addresses), subject, body - exception message
    emailobj = EmailSender()
    msg = emailobj.message(subject="Exception Occurred",
                           text="Exception Caught in ETFAnalysis/ProcessCaller.py {}".format(traceback.format_exc()))
    emailobj.send(msg=msg, receivers=['piyush888@gmail.com', 'kshitizsharmav@gmail.com'])
    pass
