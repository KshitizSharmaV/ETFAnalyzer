import os, sys
# For Piyush System
sys.path.extend(['/home/piyush/Desktop/etf0406', '/home/piyush/Desktop/etf0406/ETFAnalyzer', '/home/piyush/Desktop/etf0406/ETFAnalyzer/ETFsList_Scripts',
                 '/home/piyush/Desktop/etf0406/ETFAnalyzer/HoldingsDataScripts',
                 '/home/piyush/Desktop/etf0406/ETFAnalyzer/CommonServices',
                 '/home/piyush/Desktop/etf0406/ETFAnalyzer/CalculateETFArbitrage'])
# For Production env
sys.path.extend(['/home/ubuntu/ETFAnalyzer', '/home/ubuntu/ETFAnalyzer/ETFsList_Scripts',
                 '/home/ubuntu/ETFAnalyzer/HoldingsDataScripts', '/home/ubuntu/ETFAnalyzer/CommonServices',
                 '/home/ubuntu/ETFAnalyzer/CalculateETFArbitrage'])
sys.path.append("..")  # Remove in production - KTZ
import FlaskAPI.server
import pymongo
from pymongo.errors import PyMongoError
from bson.json_util import dumps
from MongoDB.MongoDBConnections import MongoDBConnectors
import getpass

system_username = getpass.getuser()
if system_username == 'ubuntu':
    ''' Production to Production:'''
    connection = MongoDBConnectors().get_pymongo_readWrite_production_production()
else:
    ''' Dev Local to Production ReadOnly:
        Will need to comment out create_index() statements to use this connection '''
    connection = MongoDBConnectors().get_pymongo_readonly_devlocal_production()
    ''' Dev Local to Production ReadWrite:
        Will need to specify username password file in MongoDB/MongoDBConnections.py __init__() '''
    # connection = MongoDBConnectors().get_pymongo_readWrite_devlocal_production()

db = connection.ETF_db
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