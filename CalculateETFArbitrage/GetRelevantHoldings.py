import sys  # Remove in production - KTZ
sys.path.append("..")  # Remove in production - KTZ

from CalculateETFArbitrage.LoadEtfHoldings import LoadHoldingsdata
from ETFsList_Scripts.List523ETFsMongo import ETFListDocument
from mongoengine import *
import csv

class RelevantHoldings():
    def __init__(self):
        self.listofetfs = []
        self.SetOfHoldings = set()
        self.ChineseHoldings = set()
        self.NonChineseHoldings = set()
        self.NonChineseETFs = []
    def getAllETFNames(self):
        try:
            # connect('ETF_db', alias='ETF_db')
            # Connecting to ETF_db on AWS EC2 Production Server
            connect('ETF_db', alias='ETF_db', host='52.90.110.245', port=27017)
            etflistdocument = ETFListDocument.objects().first()
            # print(etflistdocument)
            for etf in etflistdocument.etflist:
                self.listofetfs.append(str(etf.Symbol))
            # print(self.listofetfs)
        except Exception as e:
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
    RelevantHoldings().write_to_csv(non)