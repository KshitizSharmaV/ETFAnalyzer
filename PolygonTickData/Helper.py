import pandas as pd
import numpy as np
from datetime import datetime
import time
import functools


###################################################################
### Use this class for performing actions and keep code clean  ####
###################################################################

class Helper(object):

    def __init__(self):
        pass

    def convertDictToFrame(self, data):
        finalDF = []
        for key, value in data.items():
            df = pd.DataFrame.from_dict(value[key]['results'])
            df['Symbol'] = key
            finalDF.append(df)
        return pd.concat(finalDF)

    # Returns timestamp with milliseconds
    def unix_time_millis(self, dt):
        epoch = datetime.utcfromtimestamp(0)
        tsDate = (dt - epoch).total_seconds() * 1000.0
        # tsDate = str(int(tsDate)) + '000000'
        tsDate = "".join([str(int(tsDate)), '000000'])
        return tsDate

    def stringTimeToDatetime(self, date=None, time=None):
        # marketOpenTSStr = date + ' ' + time
        marketOpenTSStr = " ".join([date, time])
        return datetime.strptime(marketOpenTSStr, '%Y-%m-%d %H:%M:%S')

    def convertStringDateToTS(self, date=None, starttime='9:30:00', endtime='17:00:00'):
        # marketOpenTSStr = date + ' ' + starttime
        marketOpenTSStr = " ".join([date, starttime])
        # marketCloseTSStr = date + ' ' + endtime
        marketCloseTSStr = " ".join([date, endtime])

        marketTimeStamps = {}
        marketTimeStamps['marketOpenTS'] = self.unix_time_millis(
            datetime.strptime(marketOpenTSStr, '%Y-%m-%d %H:%M:%S'))
        marketTimeStamps['marketCloseTS'] = self.unix_time_millis(
            datetime.strptime(marketCloseTSStr, '%Y-%m-%d %H:%M:%S'))
        return marketTimeStamps

    def convertHumanTimeToUnixTimeStamp(self, date=None, time='9:30:00'):
        # marketOpenTSStr = date + ' ' + time
        datetimeobject = " ".join([date, time])
        return self.unix_time_millis(datetime.strptime(datetimeobject, '%Y-%m-%d %H:%M:%S'))

    def getHumanTime(self, ts=None, divideby=1000000000):
        if not ts:
            return None
        s, ms = divmod(ts, divideby)
        return datetime(*time.gmtime(s)[:6])

    def getLastTimeStamp(self, data):
        if data['results']:
            return data['results'][-1]['t']
        else:
            return None

    def checkTimeStampForPagination(self, checkTime, extractDataTillTime):
        if not checkTime:
            return False
        return True if self.getHumanTime(ts=checkTime, divideby=1000000000) < extractDataTillTime else False

    # vwap : Volume Weighted Average Price
    def vwap(self, df):
        q = df['Spread'].values
        p = df['Total Bid Ask Size'].values
        return df.assign(vwap=(p * q).cumsum() / q.cumsum())

    def EtfMover(self, df=None, columnName=None):
        df = round(df, 4)
        arr = df.abs().values.argsort(1)[:, -10:][:, ::-1]
        pos = 0
        length = len(df.columns)
        if length > 10:
            length = 10
        changes = [[] for i in range(length)]
        for i in arr:
            top = df.iloc[pos,].to_dict()
            l = [(k, v) for k, v in top.items()]
            for j in range(length):
                changes[j].append(l[i[j]])
            pos += 1
        result = pd.DataFrame([changes[c] for c in range(length)]).T
        result.index = df.index
        result.columns = [columnName + str(i) for i in range(1, length + 1)]
        return result
