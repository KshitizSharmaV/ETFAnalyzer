import datetime
import sys

sys.path.append("..")  # Remove in production - KTZ


class MongoTradesQuotesData(object):
    """This class is for saving Trades and Quotes Daily data in QuotesData and TradeData Collection"""

    def insert_into_collection(self, symbol=None, datetosave=None, savedata=None, CollectionName=None, batchSize=None):
        """Insert Trades/Quotes data into MongoDB"""
        print(symbol + " BatchSize is=" + str(batchSize))
        insert_data = {'symbol': symbol,
                     'dateForData': datetime.datetime.strptime(datetosave, '%Y-%m-%d'),
                     'dateWhenDataWasFetched': datetime.datetime.today(),
                     'data': savedata,
                     'batchSize': batchSize}
        CollectionName.insert_one(insert_data)

    def fetch_quotes_trades_data_from_mongo(self, symbolList=None, date=None, CollectionName=None, pipeline=None):
        """Fetch Trades/Quotes Data from MongoDB"""
        query = {'dateForData': datetime.datetime.strptime(date, '%Y-%m-%d'), 'symbol': {'$in': symbolList}}
        pipeline[0]['$match'] = query
        data_d = CollectionName.aggregate(pipeline, allowDiskUse=True)
        combineddata = [data for item in data_d for data in item['data']]
        return combineddata

    def does_item_exist_in_quotes_trades_mongo_db(self, s=None, date=None, CollectionName=None):
        """Return False if list is empty, i.e. symbol-date combination doesn't exist and it needs to be downloaded"""
        s = CollectionName.find({'symbol': s, 'dateForData': datetime.datetime.strptime(date, '%Y-%m-%d')}).count()
        return False if s == 0 else True


class MongoDailyOpenCloseData():
    """This class is for saving Daily Data - Open and Close in collection - DailyOpenCloseCollection"""

    def insert_into_collection(self, symbol=None, datetosave=None, savedata=None, CollectionName=None):
        """Insert Daily Open Close data into MongoDB"""
        insert_data = {'Symbol': symbol,
                     'dateForData': datetime.datetime.strptime(datetosave, '%Y-%m-%d'),
                     'dateWhenDataWasFetched': datetime.datetime.today(),
                     'Open Price': savedata['o'],
                     'Volume': savedata['v'],
                     'Close': savedata['c'],
                     'High': savedata['h'],
                     'Low': savedata['l'],
                     }
        CollectionName.insert_one(insert_data)

    def does_item_exist_in_daily_open_close_mongo_db(self, s=None, date=None, CollectionName=None):
        """Return False if list is empty, i.e. symbol-date combination doesn't exist and it needs to be downloaded"""
        s = CollectionName.find({'Symbol': s, 'dateForData': datetime.datetime.strptime(date, '%Y-%m-%d')}).count()
        return False if s == 0 else True

    def fetch_daily_open_close_data(self, symbolList=None, date=None, CollectionName=None):
        """Fetch Daily Open Close Data from MongoDB"""
        query = {'dateForData': datetime.datetime.strptime(date, '%Y-%m-%d'), 'Symbol': {'$in': symbolList}}
        data_d = CollectionName.find(query, {'Symbol': 1, 'Open Price': 1, '_id': 0})
        combineddata = [item for item in data_d]
        return combineddata
