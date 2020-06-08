import sys, traceback

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
import ujson
import json
import pandas as pd
import websocket

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
    # print(responses)

    dataQ = [response for response in responses if response['ev'] == 'Q']
    dataAM = [response for response in responses if response['ev'] == 'AM']
    if dataAM:
        loop = asyncio.get_event_loop()
        loop.run_until_complete(PerMinDataOperations().do_insert(dataAM))
        # PerMinDataOperations().insertDataPerMin(data)
        print("Aggregates-Minute Inserted")
    if dataQ:
        PerMinDataOperations().insertQuotesLive(dataQ)
        print("Quotes Inserted")

    end = time.time()
    print("Done in {}".format(end-start))



def on_error(ws, error):
    print("error : {}".format(error))
    print("retrying...")
    main()


def on_close(ws):
    print("Connection Closed")


def on_open(ws):
    ws.send('{"action":"auth","params":"M_PKVL_rqHZI7VM9ZYO_hwPiConz5rIklx893F"}')
    # Subscribe to ticker data
    tickerlist = list(pd.read_csv("tickerlist.csv").columns.values)
    tickerlistStr = ','.join([''.join(['AM.', str(elem)]) for elem in tickerlist])
    etflist = list(pd.read_csv("NonChineseETFs.csv").columns.values)
    quotestickerlistStr = ','.join([''.join(['Q.', str(elem)]) for elem in etflist])
    subs_list = ','.join([tickerlistStr,quotestickerlistStr])
    print(subs_list)
    print(tickerlistStr)
    subscription_data = {"action": "subscribe", "params": subs_list}
    ws.send(json.dumps(subscription_data))


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
