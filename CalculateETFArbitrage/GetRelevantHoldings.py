import sys  # Remove in production - KTZ
import traceback

sys.path.append("..")  # Remove in production - KTZ
import pandas as pd
from CalculateETFArbitrage.LoadEtfHoldings import LoadHoldingsdata
from ETFsList_Scripts.List523ETFsMongo import ETFListDocument
from MongoDB.MongoDBConnections import MongoDBConnectors
from mongoengine import *
import csv
import getpass
class RelevantHoldings():
    def __init__(self):
        self.listofetfs = []
        self.SetOfHoldings = set()
        self.ChineseHoldings = set()
        self.NonChineseHoldings = set()
        self.NonChineseETFs = []
        self.system_username = getpass.getuser()
    def getAllETFNames(self):
        try:
            if self.system_username == 'ubuntu':
                MongoDBConnectors().get_mongoengine_readWrite_production_production()
            else:
                MongoDBConnectors().get_mongoengine_readonly_devlocal_production()
            # 1484 list
            etfdf = pd.read_csv("/home/piyush/Downloads/etfs_details_type_fund_flow (4).csv")
            etfdf.set_index('Symbol', inplace=True)
            # above 1 billion list
            etflist = [symbol for symbol in etfdf.index if float(etfdf.loc[symbol,'Total Assets '][1:].replace(',',''))>1000000000]
            # 282 list
            workingetflist = pd.read_csv('~/Desktop/etf0406/ETFAnalyzer/CalculateETFArbitrage/WorkingETFs.csv').columns.to_list()
            # final common list
            self.listofetfs = list(set(etflist).union(set(workingetflist)))
            print(self.listofetfs)
            print(len(self.listofetfs))
            return self.listofetfs
        except Exception as e:
            traceback.print_exc()
            print("Can't Fetch Fund Holdings Data for all ETFs")
            print(e)

    def getAllNonChineseHoldingsETFs(self):

        self.getAllETFNames()
        for etf in self.listofetfs:
            try:
                listofholding = LoadHoldingsdata().getHoldingsDataForAllETFfromDB(etf)
                self.SetOfHoldings = self.SetOfHoldings.union(set(listofholding))
            except:
                print("Exception in {} etf".format(etf))
                continue

            self.differentiate_foreign_holdings()
            if not self.ChineseHoldings:
                self.NonChineseETFs.append(etf)

            self.ChineseHoldings.clear()
            self.NonChineseHoldings.clear()
            self.SetOfHoldings.clear()
        return self.NonChineseETFs

    def differentiate_foreign_holdings(self):
        # self.getAllHoldingsFromAllETFs()
        for holding in self.SetOfHoldings:
            try:
                x = int(holding)
                self.ChineseHoldings.add(holding)
            except ValueError:
                self.NonChineseHoldings.add(holding)
            except Exception as e:
                print("Exception for {} etf. Belongs in no category".format(holding))
                pass
        # print("Chinese Holdings : \n")
        # print(self.ChineseHoldings)
        # print("Non-Chinese Holdings : \n")
        # print(self.NonChineseHoldings)

    def write_to_csv(self, etflist, filename="NonChineseETFs.csv"):
        # name of csv file
        filename = filename
        # writing to csv file
        with open(filename, 'w') as csvfile:
            csvwriter = csv.writer(csvfile)
            csvwriter.writerow(etflist)

if __name__ == "__main__":
    non = RelevantHoldings().getAllNonChineseHoldingsETFs()
    RelevantHoldings().write_to_csv(non, 'Consolidated1blist.csv')