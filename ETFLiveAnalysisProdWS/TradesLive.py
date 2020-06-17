import datetime
import sys, traceback

sys.path.append("..")  # Remove in production - KTZ
import ujson
import json
import pandas as pd
import websocket
import logging
import os

from CommonServices.LogCreater import CreateLogger
from CommonServices import ImportExtensions

logObj = CreateLogger()
logger = logObj.createLogFile(dirName="Logs/",logFileName="-CalculateArbPerMinLog.log",loggerName="CalculateArbPerMinLog")


try:
    import thread
except ImportError:
    import _thread as thread
import time
from MongoDB.PerMinDataOperations import PerMinDataOperations
import asyncio


def on_message(ws, message):
    start = time.time()
    responses = ujson.loads(message)
    #dataQ = [response for response in responses if response['ev'] == 'Q']
    dataAM = [response for response in responses if response['ev'] == 'AM']
    if dataAM:
        loop = asyncio.get_event_loop()
        loop.run_until_complete(PerMinDataOperations().do_insert(dataAM))
        logger.debug("Aggregates-Minute Inserted")
    '''
    if dataQ:
        print("Quotes")
        print(dataQ)
        PerMinDataOperations().insertQuotesLive(dataQ)
    ''' 
    end = time.time()
    


def on_error(ws, error):
    print("error : {}".format(error))
    logger.exception(error)
    print("retrying...")
    logger.debug('retrying...')
    main()


def on_close(ws):
    print("Connection Closed")
    logger.debug("Websocket Connection Closed")


def on_open(ws):
    ws.send('{"action":"auth","params":"M_PKVL_rqHZI7VM9ZYO_hwPiConz5rIklx893F"}')
    tickerlist = list(pd.read_csv("../CSVFiles/tickerlist.csv").columns.values)
    tickerlistStr = ','.join([''.join(['AM.', str(elem)]) for elem in tickerlist])
    '''
    etflist = list(pd.read_csv("NonChineseETFs.csv").columns.values)
    quotestickerlistStr = ','.join([''.join(['Q.', str(elem)]) for elem in etflist])
    subs_list = ','.join([tickerlistStr,quotestickerlistStr])
    '''
    subscription_data = {"action": "subscribe", "params": tickerlistStr}
    ws.send(json.dumps(subscription_data))
    logger.debug("Subscribed Polygon Websocket for Live data")


def main():
    websocket.enableTrace(True)
    ws = websocket.WebSocketApp("wss://socket.polygon.io/stocks",
                                on_message=on_message,
                                on_error=on_error,
                                on_close=on_close)
    ws.on_open = on_open
    ws.run_forever()

if __name__ == "__main__":
    main()
