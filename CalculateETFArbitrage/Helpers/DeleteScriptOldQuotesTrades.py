import sys, traceback

sys.path.append('../..')
from datetime import datetime, timedelta
import CommonServices.ImportExtensions
from CommonServices.EmailService import EmailSender
import getpass
from MongoDB.MongoDBConnections import MongoDBConnectors
from CommonServices.LogCreater import CreateLogger

logObj = CreateLogger()
logger = logObj.createLogFile(dirName='Logs/', logFileName='-HistoricDeleteScriptLog.log',
                              loggerName='HistoricDeleteScriptLogger')
sys_username = getpass.getuser()
if sys_username == 'ubuntu':
    readWriteConnection = MongoDBConnectors().get_pymongo_readWrite_production_production()
else:
    readWriteConnection = MongoDBConnectors().get_pymongo_devlocal_devlocal()
db = readWriteConnection.ETF_db


def delete_old_live_data_from_collection(collectionName):
    try:
        # LAST TIMESTAMP PRESENT IN THE COLLECTION
        last = collectionName.find({}, {'dateForData': 1, '_id': 0}).sort([('dateForData', -1)]).limit(1)
        last_date = list(last)[0]['dateForData']
        print("last_date : {}".format(last_date))
        logger.debug("last_date : {}".format(last_date))

        # 2 DAYS PRIOR TIMESTAMP FOR RECORD DELETION
        del_dt = last_date - timedelta(days=2)
        print("del_dt : {}".format(del_dt))
        logger.debug("del_dt : {}".format(del_dt))

        # # DELETE DATA WITH TIMESTAMP LESS THAN EQUAL TO THIS TIMESTAMP
        status = collectionName.delete_many({'dateForData': {'$lte': del_dt}})

        print("Acknowledged : {}".format(status.acknowledged))
        logger.debug("Acknowledged : {}".format(status.acknowledged))
        print("Deleted Count: {}".format(status.deleted_count))
        logger.debug("Deleted Count: {}".format(status.deleted_count))
    except Exception as e:
        traceback.print_exc()
        logger.warning('Could not delete records from: {}'.format(collectionName))
        logger.exception(e)
        emailobj = EmailSender()
        msg = emailobj.message(subject=e,
                               text="Exception Caught in ETFLiveAnalysisProdWS/DeleteScript.py {}".format(
                                   traceback.format_exc()))
        emailobj.send(msg=msg, receivers=['piyush888@gmail.com', 'kshitizsharmav@gmail.com'])
        pass


if __name__ == '__main__':
    logger.debug("Deleting records from QuotesData")
    delete_old_live_data_from_collection(db.QuotesData)
    # logger.debug("Deleting records from TradesData")
    # delete_old_live_data_from_collection(db.TradesData)
    logger.debug("Job Finished")
