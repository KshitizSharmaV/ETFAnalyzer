import sys  # Remove in production - KTZ
import traceback

# For Piyush System
sys.path.extend(['/home/piyush/Desktop/etf0406', '/home/piyush/Desktop/etf0406/ETFAnalyzer',
                 '/home/piyush/Desktop/etf0406/ETFAnalyzer/ETFsList_Scripts',
                 '/home/piyush/Desktop/etf0406/ETFAnalyzer/HoldingsDataScripts',
                 '/home/piyush/Desktop/etf0406/ETFAnalyzer/CommonServices',
                 '/home/piyush/Desktop/etf0406/ETFAnalyzer/CalculateETFArbitrage'])
# For Production env
sys.path.extend(['/home/ubuntu/ETFAnalyzer', '/home/ubuntu/ETFAnalyzer/ETFsList_Scripts',
                 '/home/ubuntu/ETFAnalyzer/HoldingsDataScripts', '/home/ubuntu/ETFAnalyzer/CommonServices',
                 '/home/ubuntu/ETFAnalyzer/CalculateETFArbitrage'])
sys.path.append("..")  # Remove in production - KTZ
from CommonServices.EmailService import EmailSender
import pandas as pd
from datetime import datetime
from datetime import timedelta
import getpass
from CalculateETFArbitrage.LoadEtfHoldings import LoadHoldingsdata
from CalculateETFArbitrage.Control import ArbitrageCalculation
from MongoDB.SaveArbitrageCalcs import SaveCalculatedArbitrage
from CalculateETFArbitrage.GetRelevantHoldings import RelevantHoldings
from MongoDB.FetchArbitrage import FetchArbitrage
from MongoDB.MongoDBConnections import MongoDBConnectors
from CommonServices.Holidays import HolidayCheck
from CommonServices.LogCreater import CreateLogger

logger = CreateLogger().createLogFile(dirName='Logs/', logFileName='-ArbEventLog.log', loggerName='HistArbEventLogger')
logger2 = CreateLogger().createLogFile(dirName='Logs/', logFileName='-ArbErrorLog.log', loggerName='HistArbErrorLogger')

# if getpass.getuser()=='ubuntu':
#     client = MongoDBConnectors().get_pymongo_readonly_production_production()
# else:
#     client = MongoDBConnectors().get_pymongo_readonly_devlocal_production()
# result = client['ETF_db']['ArbitrageCollection'].aggregate([
#     {
#         '$group': {
#             '_id': '$dateOfAnalysis',
#             'count': {
#                 '$sum': 1
#             }
#         }
#     }
# ])
# datelist = [item['_id'].strftime('%Y-%m-%d') for item in result]
# print(datelist)
# base = datetime.today()
# datelist = [(base - timedelta(days=x)).strftime('%Y-%m-%d') for x in range(38) if HolidayCheck(base - timedelta(days=x))==False]
# dates = ['2020-06-01', '2020-06-02', '2020-06-03', '2020-06-04', '2020-06-05', '2020-06-08', '2020-06-09', '2020-06-10',
#          '2020-06-11', '2020-06-12', '2020-06-15', '2020-06-16', '2020-06-17', '2020-06-18', '2020-06-19', '2020-06-22',
#          '2020-06-23', '2020-06-24', '2020-06-25', '2020-06-26', '2020-06-29', '2020-06-30', '2020-06-01', '2020-06-02',
#          '2020-06-06', '2020-06-07']
dates = ['2020-06-12','2020-07-01','2020-07-09','2020-07-15','2020-07-21', '2020-07-23']
for date in dates:
    print(date)
    etfwhichfailed = []
    # MAKE A LIST OF WORKING ETFs.
    workingdf = pd.read_csv("../CSVFiles/250M_WorkingETFs.csv")
    # workinglist = workingdf['Symbol'].to_list()
    workinglist = workingdf.columns.to_list()
    print("List of working ETFs:")
    print(workinglist)
    print(len(workinglist))

    # CHECK ARBITRAGE COLLECTION FOR ETFs ALREADY PRESENT.
    arb_db_data = FetchArbitrage().fetch_arbitrage_data(date)
    arb_db_data_etflist = [arbdata['ETFName'] for arbdata in arb_db_data]
    arb_db_data_etflist = list(set(arb_db_data_etflist))
    print("List of ETFs whose arbitrage calculation is present in DB:")
    print(arb_db_data_etflist)
    print(len(arb_db_data_etflist))

    # REMOVE THE ETFs, FROM WORKING ETF LIST, WHOSE ARBITRAGE HAS ALREADY BEEN CALCULATED.
    print("Updated etflist:")
    workingset = set(workinglist)
    doneset = set(arb_db_data_etflist)
    etflist = list(workingset.difference(doneset))
    print(etflist)
    print(len(etflist))

    # REMOVE ALL QUOTES DATA AND TRADES DATA FOR THE UPDATED ETF LIST FOR THE GIVEN DATE
    del_list = etflist.copy()
    if getpass.getuser()=='ubuntu':
        rem_conn = MongoDBConnectors().get_pymongo_readWrite_production_production()
    else:
        rem_conn = MongoDBConnectors().get_pymongo_devlocal_devlocal()
    quotes_del = rem_conn.ETF_db.QuotesData.delete_many({'dateForData': datetime.strptime(date, '%Y-%m-%d'), 'symbol': {'$in': del_list}})
    print(quotes_del.deleted_count)
    sym_list = [del_list.extend(LoadHoldingsdata().LoadHoldingsAndClean(etf, datetime.strptime(date, '%Y-%m-%d')).getSymbols()) for etf in etflist]
    trades_del = rem_conn.ETF_db.TradesData.delete_many({'dateForData': datetime.strptime(date, '%Y-%m-%d'), 'symbol': {'$in': del_list}})
    print(trades_del.deleted_count)

    for etfname in etflist:
        try:
            print("Doing Analysis for ETF= " + etfname)
            logger.debug("Doing Analysis for ETF= {}".format(etfname))
            data = ArbitrageCalculation().calculateArbitrage(etfname, date)

            if data is None:
                print("Holding Belong to some other Exchange, No data was found")
                logger.debug("Holding Belong to some other Exchange, No data was found for {}".format(etfname))
                etfwhichfailed.append(etfname)
                continue
            else:
                data.reset_index(inplace=True)
                data['ETFName'] = etfname
                data['dateOfAnalysis'] = datetime.strptime(date, '%Y-%m-%d')
                data['dateWhenAnalysisRan'] = datetime.now()
                SaveCalculatedArbitrage().insertIntoCollection(data=data.to_dict(orient='records'))

        except Exception as e:
            etfwhichfailed.append(etfname)
            print("exception in {} etf, not crawled".format(etfname))
            print(e)
            traceback.print_exc()
            logger.exception(e)
            logger2.exception(e)
            email_obj = EmailSender()
            msg = email_obj.message(subject="Exception Occurred",
                                   text="Exception Caught in ETFAnalysis/CalculateETFArbitrage/MultipleDatesCaller.py {}".format(
                                       traceback.format_exc()))
            email_obj.send(msg=msg, receivers=['piyush888@gmail.com', 'kshitizsharmav@gmail.com'])
            continue
    if len(etfwhichfailed) > 0:
        RelevantHoldings().write_to_csv(etfwhichfailed, "etfwhichfailed.csv")

    print(etflist)
    print(etfwhichfailed)
