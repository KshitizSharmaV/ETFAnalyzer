import sys, traceback
sys.path.append('..')
from CommonServices.ImportExtensions import *
import pandas as pd
import getpass, datetime
from pymongo import *
from MongoDB.MongoDBConnections import MongoDBConnectors
from FlaskAPI.Components.ETFArbitrage.ETFArbitrageMain import AnalyzeArbitrageDataForETF
from CommonServices.LogCreater import CreateLogger

logger = CreateLogger().createLogFile(dirName='Logs/', logFileName='-PNLLog.log', loggerName='PNLLogger')


class CalculateAndSavePnLData():
    def __init__(self):
        self.sysUserName = getpass.getuser()
        if self.sysUserName == 'ubuntu':
            self.connforthis = MongoDBConnectors().get_pymongo_readWrite_production_production()
        else:
            self.connforthis = MongoDBConnectors().get_pymongo_readonly_devlocal_production()

        self.arbitragecollection = self.connforthis.ETF_db.ArbitrageCollection

    ########################################################################################
    # Use to populate DB for the first time, for all dates which are not present
    ########################################################################################
    def retrievePNLForAllDays(self, magnitudeOfArbitrageToFilterOn=0):
        try:
            date_list = self.returnres()
            date_list.sort()
            etflist = pd.read_csv('../CSVFiles/250M_WorkingETFs.csv').columns.to_list()
            # final_res = []
            for date in date_list:
                try:
                    if self.sysUserName == 'ubuntu':
                        presence = MongoDBConnectors().get_pymongo_readWrite_production_production().ETF_db.PNLDataCollection.find(
                            {'Date': date})
                    else:
                        presence = MongoDBConnectors().get_pymongo_readonly_devlocal_production().ETF_db.PNLDataCollection.find(
                            {'Date': date})

                    presence_list = list(presence)
                    etf_already_present = [item['Symbol'] for item in presence_list]
                    etf_to_be_done = list(set(etflist)-set(etf_already_present))
                    if len(presence_list)==len(etflist):
                        continue
                    all_etf_arb_cursor = self.arbitragecollection.find({'dateOfAnalysis': date})
                    PNLOverDates = {}
                    final_res = []
                    # Iter over the collection results
                    for etf_arb in all_etf_arb_cursor:
                        if etf_arb['ETFName'] in etf_to_be_done:
                            try:
                                print(etf_arb['ETFName'])
                                logger.debug(etf_arb['ETFName'])
                                allData, pricedf, pnlstatementforday, scatterPlotData = AnalyzeArbitrageDataForETF(
                                    arbitrageDataFromMongo=etf_arb,
                                    magnitudeOfArbitrageToFilterOn=magnitudeOfArbitrageToFilterOn)
                                PNLOverDates[str(etf_arb['ETFName'])] = pnlstatementforday
                            except Exception as e0:
                                print("Exception in {}".format(etf_arb['ETFName']))
                                logger.warning("Exception in {}".format(etf_arb['ETFName']))
                                print(e0)
                                traceback.print_exc()
                                logger.exception(e0)
                                pass
                    PNLOverDates = pd.DataFrame(PNLOverDates).T
                    # del PNLOverDates['Magnitue Of Arbitrage']
                    PNLOverDates.columns = ['Sell Return%', 'Buy Return%', 'Magnitue Of Arbitrage', '# T_Buy',
                                            '# R_Buy', '# T_Sell', '# R_Sell']
                    PNLOverDates['% R_Buy'] = round(PNLOverDates['# R_Buy'] / PNLOverDates['# T_Buy'], 2)
                    PNLOverDates['% R_Sell'] = round(PNLOverDates['# R_Sell'] / PNLOverDates['# T_Sell'], 2)
                    PNLOverDates['Date'] = date
                    PNLOverDates = PNLOverDates[
                        ['Date', 'Sell Return%', 'Buy Return%', '# T_Buy', '# R_Buy', '% R_Buy', '# T_Sell', '# R_Sell',
                         '% R_Sell', 'Magnitue Of Arbitrage']]
                    final_res.extend(PNLOverDates.reset_index().rename(columns={'index': 'Symbol'}).to_dict('records'))
                    print(final_res)
                    self.Save_PnLData(final_res)
                except Exception as e:
                    traceback.print_exc()
                    logger.exception(e)
                    # print("Exception in {}".format(etf_arb['ETFName']))
                    pass
        except  Exception as e1:
            logger.exception(e1)
            traceback.print_exc()
            pass
        # return final_res

    ########################################################################################
    # Will run daily once after Historical Arbitrage to store PNL of last calculated Historical Arb for all ETF
    ########################################################################################
    def retrievePNLForAllETF_ForOneDay(self, magnitudeOfArbitrageToFilterOn=0):
        last_date_cursor = self.arbitragecollection.find({}, {'_id': 0, 'dateOfAnalysis': 1}).sort(
            [('dateOfAnalysis', -1)]).limit(1)
        last_date = list(last_date_cursor)[0]['dateOfAnalysis']
        all_etf_arb_cursor = self.arbitragecollection.find({'dateOfAnalysis': last_date})
        PNLOverDates = {}
        # Iter over the collection results
        for etf_arb in all_etf_arb_cursor:
            allData, pricedf, pnlstatementforday, scatterPlotData = AnalyzeArbitrageDataForETF(
                arbitrageDataFromMongo=etf_arb, magnitudeOfArbitrageToFilterOn=magnitudeOfArbitrageToFilterOn)
            PNLOverDates[str(etf_arb['ETFName'])] = pnlstatementforday
        PNLOverDates = pd.DataFrame(PNLOverDates).T
        # del PNLOverDates['Magnitue Of Arbitrage']
        PNLOverDates.columns = ['Sell Return%', 'Buy Return%', 'Magnitue Of Arbitrage', '# T_Buy', '# R_Buy',
                                '# T_Sell', '# R_Sell']
        PNLOverDates['% R_Buy'] = round(PNLOverDates['# R_Buy'] / PNLOverDates['# T_Buy'], 2)
        PNLOverDates['% R_Sell'] = round(PNLOverDates['# R_Sell'] / PNLOverDates['# T_Sell'], 2)
        PNLOverDates['Date'] = last_date
        PNLOverDates = PNLOverDates[
            ['Date', 'Sell Return%', 'Buy Return%', '# T_Buy', '# R_Buy', '% R_Buy', '# T_Sell', '# R_Sell', '% R_Sell',
             'Magnitue Of Arbitrage']]
        return PNLOverDates.reset_index().rename(columns={'index': 'Symbol'}).to_dict('records')

    def returnres(self):
        curs = self.arbitragecollection.aggregate([
            {
                '$group': {
                    '_id': '$dateOfAnalysis',
                    'count': {
                        '$sum': 1
                    }
                }
            }
        ])
        res = list(curs)
        dates = [item['_id'] for item in res]
        return (dates)

    def Save_PnLData(self, data):
        if self.sysUserName == 'ubuntu':
            result = self.connforthis.ETF_db.PNLDataCollection.insert_many(data)
        else:
            result = MongoDBConnectors().get_pymongo_devlocal_devlocal().ETF_db.PNLDataCollection.insert_many(data)
        print('inserted %d docs' % (len(result.inserted_ids),))
        logger.debug('inserted %d docs' % (len(result.inserted_ids),))


if __name__ == '__main__':
    obj = CalculateAndSavePnLData()
    # obj.Save_PnLData(obj.retrievePNLForAllETF_ForOneDay())
    obj.retrievePNLForAllDays()
