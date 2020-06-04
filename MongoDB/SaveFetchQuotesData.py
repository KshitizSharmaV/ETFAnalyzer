##########
# Save and Fetch Quotes Data
##########
import sys
sys.path.append("..")  # Remove in production - KTZ

import datetime
from mongoengine.queryset.visitor import Q
import json
from bson import json_util

# This class is for saving Trades and Quotes Daily data in QuotesData and TradeData Collection
class MongoTradesQuotesData(object):

    def __init__(self):
        pass
    
    def insertIntoCollection(self, symbol=None, datetosave=None, savedata=None, CollectionName=None, batchSize=None):
        print(symbol + " BatchSize is=" + str(batchSize))
        inserData={'symbol':symbol, 
                'dateForData':datetime.datetime.strptime(datetosave,'%Y-%m-%d'), 
                'dateWhenDataWasFetched': datetime.datetime.today(),
                'data':savedata,
                'batchSize':batchSize}
        CollectionName.insert_one(inserData)

    def fetchQuotesTradesDataFromMongo(self, symbolList=None, date=None, CollectionName=None, pipeline=None):
        query={'dateForData':datetime.datetime.strptime(date,'%Y-%m-%d'), 'symbol': { '$in': symbolList }}
        pipeline[0]['$match']=query
        # Cursor
        dataD = CollectionName.aggregate(pipeline,allowDiskUse=True)
        
        combineddata=[]
        [combineddata.extend(item['data']) for item in dataD]
        return combineddata
    
    def doesItemExsistInQuotesTradesMongoDb(self, s=None, date=None, CollectionName=None):
        s= CollectionName.find({'symbol':s,'dateForData':datetime.datetime.strptime(date,'%Y-%m-%d')}).count()
        # Return False is list is empty, that mean symbol, date combination doesn't exsist and it needs to be downloaded
        return False if s==0 else True

# This class is for saving Daily Data - Open and close in collection - dailyopenloseCollection
class MongoDailyOpenCloseData(MongoTradesQuotesData):

    def insertIntoCollection(self, symbol=None, datetosave=None, savedata=None, CollectionName=None):
        inserData={'Symbol':symbol, 
                'dateForData':datetime.datetime.strptime(datetosave,'%Y-%m-%d'), 
                'dateWhenDataWasFetched': datetime.datetime.today(),
                'Open Price':savedata['o'],
                'Volume':savedata['v'],
                'Close':savedata['c'],
                'High':savedata['h'],
                'Low':savedata['l'],
                }
        CollectionName.insert_one(inserData)

    def doesItemExsistInQuotesTradesMongoDb(self, s=None, date=None, CollectionName=None):
        s= CollectionName.find({'Symbol':s,'dateForData':datetime.datetime.strptime(date,'%Y-%m-%d')}).count()
        # Return False is list is empty, that mean symbol, date combination doesn't exsist and it needs to be downloaded
        return False if s==0 else True

    def fetchDailyOpenCloseData(self, symbolList=None, date=None, CollectionName=None):
        query={'dateForData':datetime.datetime.strptime(date,'%Y-%m-%d'), 'Symbol': { '$in': symbolList }}
        #explain=(CollectionName.find(query).explain())
        #print(json.dumps(explain, indent=2,default=json_util.default))
        # Cursor
        dataD = CollectionName.find(query,{'Symbol':1,'Open Price':1,'_id':0})
        combineddata=[]
        for item in dataD:
            combineddata.append(item)
        return combineddata
    

        





