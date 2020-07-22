import datetime
from MongoDB.Schemas import arbitragecollection, arbitrage_per_min


class SaveCalculatedArbitrage():
    def insertIntoCollection(self, ETFName=None, dateOfAnalysis=None, data=None, dateWhenAnalysisRan=None):
        print("Saving {} etf into DB...".format(ETFName))
        inserData = {'ETFName': ETFName,
                     'dateOfAnalysis': dateOfAnalysis,
                     'dateWhenAnalysisRan': dateWhenAnalysisRan,
                     'data': data
                     }
        arbitragecollection.insert_one(inserData)

    def  insertIntoPerMinCollection(self, end_ts=None, ArbitrageData=None):
        print("Saving in Arbitrage Per Min Collection for {}".format(end_ts))
        inserData = ArbitrageData
        arbitrage_per_min.insert_many(inserData)
