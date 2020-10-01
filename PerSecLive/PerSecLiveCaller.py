import sys

sys.path.append('..')

from itertools import chain
import time
from datetime import datetime, timedelta
from CalculateETFArbitrage.Helpers.LoadEtfHoldings import LoadHoldingsdata
from PerSecLive.CalcPer10Sec import main_runner
from PerSecLive.DataFetch import FetchAndSaveHistoricalPerSecData, runner_for_etf
from CommonServices.LogCreater import CreateLogger

logObj = CreateLogger()
logger = logObj.createLogFile(dirName="PerSecLive/", logFileName="-PerSecLiveCaller.log",
                              loggerName="PerSecLiveCallerLog")


def runner():
    try:
        """Date for PerSecLive Operation -- (MODIFY BEFORE USE) in string format"""
        _date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

        """ETF List (MODIFIABLE)"""
        top_10_etf_list = ['SPY', 'IVV', 'VOO', 'QQQ', 'VUG', 'VTV', 'IJH', 'IJR', 'VGT', 'VO']

        """Get Unique tickers from all ETFs"""
        load_holdings_object = LoadHoldingsdata()
        unique_ticker_list = [load_holdings_object.LoadHoldingsAndClean(etfname=etf, fundholdingsdate=_date).getSymbols()
                              for etf in top_10_etf_list]
        unique_ticker_list = list(chain.from_iterable(unique_ticker_list))
        unique_ticker_list = [ticker.split(' ')[0] for ticker in unique_ticker_list]
        unique_ticker_list = list(set(unique_ticker_list))

        """Data Fetch from Polygon API"""
        checkpoint = time.time()
        FetchAndSaveHistoricalPerSecData().all_process_runner_trades(symbols=unique_ticker_list, date_for=_date)
        for etf in top_10_etf_list:
            runner_for_etf(etf_name=etf, date_for=_date)
        logger.debug(f"Total data fetch time : {time.time() - checkpoint} seconds")
        print(f"Total data fetch time : {time.time() - checkpoint} seconds")

        """Arbitrage Calculation and Store in DB"""
        checkpoint = time.time()
        main_runner(etf_list=top_10_etf_list, _date=datetime.strptime(_date, '%Y-%m-%d'), ticker_list=unique_ticker_list)
        logger.debug(f"Total calculation time : {time.time()-checkpoint} seconds")
        print(f"Total calculation time : {time.time()-checkpoint} seconds")
    except Exception as e:
        logger.exception(e)
        print(e)
        pass


if __name__ == "__main__":
    runner()
