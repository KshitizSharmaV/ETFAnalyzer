import sys

sys.path.append('..')
from datetime import datetime, timedelta
import pandas as pd
from CommonServices.ImportExtensions import *
from MongoDB.MongoDBConnections import MongoDBConnectors
from CommonServices.EmailService import EmailSender
from CommonServices.Holidays import HolidayCheck
import getpass


class AllCollectionsStatusCheck():
    def __init__(self):
        self.etflist = pd.read_csv('../CSVFiles/250M_WorkingETFs.csv').columns.to_list()
        if getpass.getuser() == 'ubuntu':
            self.connection = MongoDBConnectors().get_pymongo_readonly_production_production()
        else:
            self.connection = MongoDBConnectors().get_pymongo_readonly_devlocal_production()
        self.connection = MongoDBConnectors().get_pymongo_devlocal_devlocal()
        self.db = self.connection.ETF_db
        pass

    def Historic_Arbitrage_Status_Check(self, date_to_check_in_string=None):
        collection = self.db.ArbitrageCollectionNew
        date_to_check = datetime.strptime(date_to_check_in_string, '%Y%m%d')
        result_cursor = collection.find({'dateOfAnalysis': date_to_check}, {'_id': 0, 'ETFName': 1})
        result = [etf['ETFName'] for etf in result_cursor]
        result = set(result)
        result = list(result)
        print("Total {} ETFs' Historical Arbitrage was Successful for {}".format(len(result), date_to_check_in_string))
        etfwhichfailed = set(self.etflist) - set(result)
        if len(etfwhichfailed) > 0:
            print("{} ETFs which failed are : {}".format(len(etfwhichfailed), list(etfwhichfailed)))
        return result, etfwhichfailed, "Total {} ETFs' Historical Arbitrage was Successful for {}\n{} ETFs which failed are : {}".format(
            len(result), date_to_check_in_string, len(etfwhichfailed), list(etfwhichfailed))

    def ETF_Holdings_Status_Check(self, date_of_scraping_in_string=None):
        collection = self.db.ETFHoldings
        date_of_scraping = datetime.strptime(date_of_scraping_in_string, '%Y%m%d')
        result_cursor = collection.find({'DateOfScraping': date_of_scraping},
                                        {'_id': 0, 'FundHoldingsDate': 1, 'ETFTicker': 1})
        result = list(result_cursor)
        print("Holdings of total {} ETFs were scraped on {}".format(len(result), date_of_scraping_in_string))
        fhdates = set([datetime.strftime(item['FundHoldingsDate'], '%Y-%m-%d') for item in result])
        print("ETFs scraped had Fund Holdings dates of : {}".format(fhdates))
        return result, fhdates, "Holdings of total {} ETFs were scraped on {}\nETFs scraped had Fund Holdings dates of : {}".format(
            len(result), date_of_scraping_in_string, fhdates)

    def PNL_Data_Status_Check(self, date_to_check_in_string=None):
        collection = self.db.PNLDataCollection
        date_to_check = datetime.strptime(date_to_check_in_string, '%Y%m%d')
        result_cursor = collection.find({'Date': date_to_check}, {'_id': 0, 'Symbol': 1})
        result = [etf['Symbol'] for etf in result_cursor]
        print("Total {} ETFs' PNL Data was Successfully calculated and stored for {}".format(len(result),
                                                                                             date_to_check_in_string))
        etfwhichfailed = set(self.etflist) - set(result)
        if len(etfwhichfailed) > 0:
            print("{} ETFs which failed are : {}".format(len(etfwhichfailed), list(etfwhichfailed)))
        return result, etfwhichfailed, "Total {} ETFs' PNL Data was Successful for {}\n{} ETFs for which PNL failed are : {}".format(
            len(result), date_to_check_in_string, len(etfwhichfailed), list(etfwhichfailed))

    def Live_Data_Status_Check(self):
        QuotesColl = self.db.QuotesLiveData
        TradesColl = self.db.TradePerMinWS
        ArbLiveColl = self.db.ArbitragePerMin

        QuotesCollFrom = list(QuotesColl.find({}).sort([('timestamp', 1)]).limit(1))[0]['timestamp']
        QuotesCollTo = list(QuotesColl.find({}).sort([('timestamp', -1)]).limit(1))[0]['timestamp']

        TradesCollFrom = list(TradesColl.find({}).sort([('e', 1)]).limit(1))[0]['e']
        TradesCollTo = list(TradesColl.find({}).sort([('e', -1)]).limit(1))[0]['e']

        ArbLiveCollFrom = list(ArbLiveColl.find({}).sort([('Timestamp', 1)]).limit(1))[0]['Timestamp']
        ArbLiveCollTo = list(ArbLiveColl.find({}).sort([('Timestamp', -1)]).limit(1))[0]['Timestamp']

        print("Quotes Live: From {} local time To {} local time".format(datetime.fromtimestamp(QuotesCollFrom / 1000),
                                                                        datetime.fromtimestamp(QuotesCollTo / 1000)))
        print("Trades Live: From {} local time To {} local time".format(datetime.fromtimestamp(TradesCollFrom / 1000),
                                                                        datetime.fromtimestamp(TradesCollTo / 1000)))
        print(
            "Arbitrage Live: From {} local time To {} local time".format(datetime.fromtimestamp(ArbLiveCollFrom / 1000),
                                                                         datetime.fromtimestamp(ArbLiveCollTo / 1000)))
        return QuotesCollFrom, QuotesCollTo, TradesCollFrom, TradesCollTo, ArbLiveCollFrom, ArbLiveCollTo, "Quotes Live: From {} local time To {} local time\nTrades Live: From {} local time To {} local time\nArbitrage Live: From {} local time To {} local time".format(
            datetime.fromtimestamp(QuotesCollFrom / 1000),
            datetime.fromtimestamp(QuotesCollTo / 1000), datetime.fromtimestamp(TradesCollFrom / 1000),
            datetime.fromtimestamp(TradesCollTo / 1000), datetime.fromtimestamp(ArbLiveCollFrom / 1000),
            datetime.fromtimestamp(ArbLiveCollTo / 1000))

    def All_Status_Report(self, date_to_check_in_string=datetime.today().strftime('%Y%m%d')):
        result_histarb, etfwhichfailed_histarb, text_hist_arb = self.Historic_Arbitrage_Status_Check(
            date_to_check_in_string=(datetime.strptime(date_to_check_in_string, '%Y%m%d') - timedelta(days=1)).strftime(
                '%Y%m%d'))
        result_ETF, fhdates, text_holdings = self.ETF_Holdings_Status_Check(
            date_of_scraping_in_string=date_to_check_in_string)
        result_PNL, etfwhichfailed_PNL, text_PNL = self.PNL_Data_Status_Check(
            date_to_check_in_string=(datetime.strptime(date_to_check_in_string, '%Y%m%d') - timedelta(days=1)).strftime(
                '%Y%m%d'))
        QuotesCollFrom, QuotesCollTo, TradesCollFrom, TradesCollTo, ArbLiveCollFrom, ArbLiveCollTo, text_live = self.Live_Data_Status_Check()
        # emailobj = EmailSender()
        # msg = emailobj.message(subject='Daily Status Report ETFAnalyzer',
        #                        text="\n".join([text_hist_arb, text_holdings, text_PNL, text_live]))
        # emailobj.send(msg=msg, receivers=['piyush888@gmail.com', 'kshitizsharmav@gmail.com'])


if __name__ == '__main__':
    # date_to_check = input("Enter date to check (Historical Arb report will be shown for the previous day): ")
    # # base = datetime.today()
    # # date_list = [base - timedelta(days=x) for x in range(10)]
    # # for date_to_check in date_list:
    # if date_to_check and HolidayCheck(date_to_check)==False:
    #     AllCollectionsStatusCheck().All_Status_Report(date_to_check_in_string=date_to_check.strftime('%Y%m%d'))
    #     print("#################################################################################")
    # else:
    #     AllCollectionsStatusCheck().All_Status_Report()
    AllCollectionsStatusCheck().All_Status_Report()
