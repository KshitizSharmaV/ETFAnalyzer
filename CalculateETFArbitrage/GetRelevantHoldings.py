import sys  # Remove in production - KTZ
import traceback

sys.path.extend(['/home/piyush/Desktop/etf0406', '/home/piyush/Desktop/etf0406/ETFAnalyzer',
                 '/home/piyush/Desktop/etf0406/ETFAnalyzer/ETFsList_Scripts',
                 '/home/piyush/Desktop/etf0406/ETFAnalyzer/HoldingsDataScripts',
                 '/home/piyush/Desktop/etf0406/ETFAnalyzer/CommonServices',
                 '/home/piyush/Desktop/etf0406/ETFAnalyzer/CalculateETFArbitrage'])
sys.path.append("..")  # Remove in production - KTZ
import pandas as pd
from CalculateETFArbitrage.LoadEtfHoldings import LoadHoldingsdata
from MongoDB.MongoDBConnections import MongoDBConnectors
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
            etfdf = pd.read_csv("../CSVFiles/250M_WorkingETFs.csv")
            self.listofetfs = etfdf['Symbol']
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
    RelevantHoldings().write_to_csv(non, 'NonChineseETFs.csv')
