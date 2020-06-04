import sys, traceback

# For Piyush System
sys.path.extend(['/home/piyush/Desktop/etf1903', '/home/piyush/Desktop/etf1903/ETFsList_Scripts',
                 '/home/piyush/Desktop/etf1903/HoldingsDataScripts',
                 '/home/piyush/Desktop/etf1903/CommonServices',
                 '/home/piyush/Desktop/etf1903/CalculateETFArbitrage', '/home/piyush/Desktop/etf1903/ETFLiveAnalysis'])
# For Production env
sys.path.extend(['/home/ubuntu/ETFAnalysis', '/home/ubuntu/ETFAnalysis/ETFsList_Scripts',
                 '/home/ubuntu/ETFAnalysis/HoldingsDataScripts', '/home/ubuntu/ETFAnalysis/CommonServices',
                 '/home/ubuntu/ETFAnalysis/CalculateETFArbitrage'])
sys.path.append("..")  # Remove in production - KTZ

import datetime
import time
import logging
import os
from ETFLiveAnalysis.ArbitragePerMin import LiveArbitragePerMinute
from MongoDB.SaveArbitrageCalcs import SaveCalculatedArbitrage
from ETFLiveAnalysis.TickListsCreatorScript import ListsCreator

path = os.path.join(os.getcwd(), "Logs/")
if not os.path.exists(path):
    os.makedirs(path)

filename = path + datetime.datetime.now().strftime("%Y%m%d") + "-ArbPerMinLog.log"
handler = logging.FileHandler(filename)
logging.basicConfig(filename=filename, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', filemode='a')
# logger = logging.getLogger("EventLogger")
logger = logging.getLogger("ArbPerMinLogger")
logger.setLevel(logging.DEBUG)
logger.addHandler(handler)


try:
    # TO MAKE LIST FILES.
    # ListsCreator().create_list_files()
    livarb = LiveArbitragePerMinute()
    # livarb.maketickerlists()
    # livarb.makeurllists()
    while True:
        dt = datetime.datetime.now() + datetime.timedelta(minutes=1)
        resultgen = livarb.main()
        resultdf = next(resultgen)
        print("ResultDF : ")
        print(resultdf)
        logger.debug("{} : PRINTED RESULT DF".format(datetime.datetime.now()))
        for idx in resultdf.index:
            DateTimeOfArbitrage = datetime.datetime.now()
            ETFName = idx
            Arbitrage = resultdf.loc[idx, 'arbitrage']
            Spread = resultdf.loc[idx, 'Spread']
            SaveCalculatedArbitrage().insertIntoPerMinCollection(DateTimeOfArbitrage=DateTimeOfArbitrage,
                                                               ETFName=ETFName,
                                                               Arbitrage=Arbitrage, Spread=Spread)

        logger.debug("{} : SAVED RESULT DF".format(datetime.datetime.now()))
        print(dt - datetime.datetime.now())
        while datetime.datetime.now() < dt:
            time.sleep(1)
except Exception as e:
    logger.exception(e)
    traceback.print_exc()
