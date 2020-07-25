import requests
from datetime import datetime
import requests
from datetime import datetime
import json
import time
import pandas as pd
from time import mktime


class PolgonDataCreateURLS(object):

    def __init__(self):
        self.params=params = (('apiKey', 'qOKbrjPAxnTvs4_hwPi GoFzgHgxgmyafq'),)

    def PolygonLastQuotes(self, symbol):
        requesturl = 'https://api.polygon.io/v1/last_quote/stocks/'+symbol+'?apiKey=qOKbrjPAxnTvs4_hwPi GoFzgHgxgmyafq'
        return requesturl

    def PolygonLastTrades(self,symbol):
        # Make use of Tickers
        requesturl='https://api.polygon.io/v1/last/stocks/'+symbol+'?apiKey=qOKbrjPAxnTvs4_hwPi GoFzgHgxgmyafq'
        return requesturl

    def PolygonHistoricQuotes(self, date=None, symbol=None,startTS=None,endTS=None,limitresult=10, aggregateBy=None):
        if startTS:
            # For Getting Paginated Request
            requesturl='https://api.polygon.io/v2/ticks/stocks/nbbo/'+symbol+'/'+date+'?timestamp='+startTS+'&timestampLimit='+endTS+'&limit='+limitresult+'&apiKey=qOKbrjPAxnTvs4_hwPi GoFzgHgxgmyafq'
            print("Paginated Request For = " + symbol)
        else:
            requesturl='https://api.polygon.io/v2/ticks/stocks/nbbo/'+symbol+'/'+date+'?timestampLimit='+endTS+'&limit='+limitresult+'&apiKey=qOKbrjPAxnTvs4_hwPi GoFzgHgxgmyafq'
            print("First Request For = " + symbol)
        return requesturl

    def PolygonHistoricTrades(self, date=None, symbol=None,startTS=None,endTS=None,limitresult=10):
        if startTS:
            # For Getting Paginated Request
            requesturl='https://api.polygon.io/v2/ticks/stocks/trades/'+symbol+'/'+date+'?timestamp='+startTS+'&timestampLimit='+endTS+'&limit='+limitresult+'&apiKey=qOKbrjPAxnTvs4_hwPi GoFzgHgxgmyafq'
            print("Paginated Request For = " + symbol)
        else:
            requesturl='https://api.polygon.io/v2/ticks/stocks/trades/'+symbol+'/'+date+'?timestampLimit='+endTS+'&limit='+limitresult+'&apiKey=qOKbrjPAxnTvs4_hwPi GoFzgHgxgmyafq'
            print("First Request For = " + symbol)
        return requesturl

    def PolygonDailyOpenClose(self,date=None, symbol=None):
        requesturl='https://api.polygon.io/v1/open-close/'+symbol+'/'+date+'?apiKey=qOKbrjPAxnTvs4_hwPi GoFzgHgxgmyafq'
        return requesturl


    def PolygonAggregdateData(self, symbol=None, aggregateBy=None, startDate=None, endDate=None):
        # Make use of Tickers, Date and Limit
        requesturl='https://api.polygon.io/v2/aggs/ticker/'+symbol+'/range/1/'+aggregateBy+'/'+startDate+'/'+endDate+'?apiKey=qOKbrjPAxnTvs4_hwPi GoFzgHgxgmyafq'
        return requesturl

    def PolygonTickTrades(self,symbolList=None):
        requesturl="wss://api.polygon.io/v1/last/stocks/"
        def on_message(ws, message):
            print(message)

        def on_error(ws, error):
            print(error)

        def on_close(ws):
            print("### closed ###")

        def on_open(ws):
            ws.send('{"action":"auth","params":"qOKbrjPAxnTvs4_hwPi GoFzgHgxgmyafq"}')
            #!!!! replace by symbolList later
            ws.send('{"action":"subscribe","params":"T.MSFT", "T.AAPL", "T.AMD", "T.NVDA"}')

        def runLiveTickData(self):
            websocket.enableTrace(True)
            ws = websocket.WebSocketApp(requesturl,
                                      on_message = on_message,
                                      on_error = on_error,
                                      on_close = on_close)
            ws.on_open = on_open
            ws.run_forever()

















