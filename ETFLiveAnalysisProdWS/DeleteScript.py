import sys, traceback
sys.path.append('..')
from datetime import datetime, timedelta
import CommonServices.ImportExtensions
import getpass
from MongoDB.MongoDBConnections import MongoDBConnectors
from CommonServices.LogCreater import  CreateLogger
logObj = CreateLogger()
logger = logObj.createLogFile(dirName='Logs/', logFileName='-DeleteScriptLog.log', loggerName='DeleteScriptLogger')
sys_username = getpass.getuser()
if sys_username == 'ubuntu':
    readWriteConnection = MongoDBConnectors().get_pymongo_readWrite_production_production()
else :
    readWriteConnection = MongoDBConnectors().get_pymongo_devlocal_devlocal()
db = readWriteConnection.ETF_db

def delete_old_live_data_from_collection(collectionName):
    try:
        # LAST TIMESTAMP PRESENT IN THE COLLECTION
        if collectionName == db.TradePerMinWS:
            last = collectionName.find({},{'e':1, '_id':0}).sort([('e',-1)]).limit(1)
            last_ts = list(last)[0]['e']
        elif collectionName == db.ArbitragePerMin:
            last = collectionName.find({}, {'Timestamp': 1, '_id': 0}).sort([('Timestamp', -1)]).limit(1)
            last_ts = list(last)[0]['Timestamp']
        else:
            last = collectionName.find({}, {'timestamp': 1, '_id': 0}).sort([('timestamp', -1)]).limit(1)
            last_ts = list(last)[0]['timestamp']
        print(int(last_ts))
        last_dt = datetime.fromtimestamp(float(last_ts)/1000)
        print("last_dt : {}".format(last_dt))
        logger.debug("last_dt : {}".format(last_dt))
        print("last_ts : {}".format(int(last_ts)))
        logger.debug("last_ts : {}".format(int(last_ts)))
        # 2 DAYS PRIOR TIMESTAMP FOR RECORD DELETION
        del_dt = last_dt - timedelta(days=2)
        print("del_dt : {}".format(del_dt))
        logger.debug("del_dt : {}".format(del_dt))
        del_ts = int(del_dt.timestamp() * 1000)
        print("del_ts : {}".format(del_ts))
        logger.debug("del_ts : {}".format(del_ts))

        # DELETE DATA WITH TIMESTAMP LESS THAN EQUAL TO THIS TIMESTAMP
        if collectionName == db.TradePerMinWS:
            status = collectionName.delete_many({'e':{'$lte':del_ts}})
        elif collectionName == db.ArbitragePerMin:
            status = collectionName.delete_many({'Timestamp':{'$lte':del_ts}})
        else:
            status = collectionName.delete_many({'timestamp':{'$lte':del_ts}})

        print("Acknowledged : {}".format(status. acknowledged))
        logger.debug("Acknowledged : {}".format(status. acknowledged))
        print("Deleted Count: {}".format(status.deleted_count))
        logger.debug("Deleted Count: {}".format(status.deleted_count))
    except Exception as e:
        traceback.print_exc()
        logger.warning('Could not delete records from: {}'.format(collectionName))
        logger.exception(e)
        pass

if __name__=='__main__':
    logger.debug("Deleting records from QuotesLiveData")
    delete_old_live_data_from_collection(db.QuotesLiveData)
    logger.debug("Deleting records from TradePerMinWS")
    delete_old_live_data_from_collection(db.TradePerMinWS)
    logger.debug("Deleting records from ArbitragePerMin")
    delete_old_live_data_from_collection(db.ArbitragePerMin)
    logger.debug("Job Finished")