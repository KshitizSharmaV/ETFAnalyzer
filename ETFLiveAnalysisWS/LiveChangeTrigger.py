import os, sys
# For Piyush system
sys.path.extend(['/home/piyush/Desktop/etf1903', '/home/piyush/Desktop/etf1903/ETFsList_Scripts',
                 '/home/piyush/Desktop/etf1903/HoldingsDataScripts',
                 '/home/piyush/Desktop/etf1903/CommonServices',
                 '/home/piyush/Desktop/etf1903/CalculateETFArbitrage'])
# For Production env
sys.path.extend(['/home/ubuntu/ETFAnalysis', '/home/ubuntu/ETFAnalysis/ETFsList_Scripts',
                 '/home/ubuntu/ETFAnalysis/HoldingsDataScripts', '/home/ubuntu/ETFAnalysis/CommonServices',
                 '/home/ubuntu/ETFAnalysis/CalculateETFArbitrage'])
import FlaskAPI.server
import pymongo
from pymongo.errors import PyMongoError
from bson.json_util import dumps

# client = pymongo.MongoClient('18.213.229.80',27017)
# Connect with replica set
client = pymongo.MongoClient('localhost',27017, replicaSet='rs0')
# Connect without replica set for now
# client = pymongo.MongoClient('localhost',27017)
db = client.ETF_db
def live_data_trigger():
    try:
        i=0
        for insert_change in db.ArbitragePerMin.watch(
                [{'$match': {'operationType': 'insert'}}]):
            print(i)
            fullDocument = insert_change['fullDocument']
            del fullDocument['_id']
            print(fullDocument)
            print(type(fullDocument))
            # FlaskAPI.server.SendLiveArbitrageDataAllTickers(fullDocument)
            i+=1
            # yield "data:{}\n\n".format(fullDocument)
    except PyMongoError:
        # The ChangeStream encountered an unrecoverable error or the
        # resume attempt failed to recreate the cursor.
        # log.error('...')
        print("Error")
live_data_trigger()