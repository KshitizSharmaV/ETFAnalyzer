from pymongo import MongoClient
import pandas as pd
import datetime
import time
import numpy as np
import talib

# Load the ETF Prices for the ETF to be analyzed
def LoadETFPrices(etfname, dateOfAnalysis, year, TradesData):
    starthour=dateOfAnalysis.strftime('%Y-%m-%d')+' 08:59:00'
    endhour=dateOfAnalysis.strftime('%Y-%m-%d')+' 16:01:00'
    def getHumanTime(ts=None, divideby=1000000000):
        s, ms = divmod(ts, divideby)
        return datetime.datetime(*time.gmtime(s)[:6])

    etfpricesData={}
    prices = TradesData.find_one({"symbol":etfname, 'dateForData':dateOfAnalysis})
    pricedf = pd.DataFrame(prices['data'])
    pricedf['t'] = pricedf['t'].apply(lambda x: getHumanTime(ts=x, divideby=1000))
    pricedf = pricedf.rename(columns={'t':'Time','o':'Open','h':'High','c':'Close','l':'Low','v':'Volume'})
    pricedf = pricedf[['Time','Volume','Open','Close','High','Low']]

    if dateOfAnalysis > datetime.datetime(year-1,9,1) and dateOfAnalysis < datetime.datetime(year,3,8):
        pricedf['Time']=pricedf['Time'] - datetime.timedelta(hours=5)
    else:
        pricedf['Time']=pricedf['Time'] - datetime.timedelta(hours=4)
    return pricedf[(pricedf['Time']>starthour) & (pricedf['Time']<endhour)]


# Load the Arbitrage Data
# 1) Method is called first time with i['data'] hence the name
# 2) Function is used to convert the time from UTC to ES
def LoadETFArbitrageData(etfdata,dateOfAnalysis,year):
    starthour=dateOfAnalysis.strftime('%Y-%m-%d')+' 08:59:00'
    endhour=dateOfAnalysis.strftime('%Y-%m-%d')+' 16:01:00'

    etfdata = pd.DataFrame(etfdata)
    # Convert UTC time to E"ST time, Check if winter time - 5, Summer time - 4 
    if dateOfAnalysis > datetime.datetime(year-1,9,1) and dateOfAnalysis < datetime.datetime(year,3,8):
        etfdata['Time']=etfdata['Time'] - datetime.timedelta(hours=5)
    else:
        etfdata['Time']=etfdata['Time'] - datetime.timedelta(hours=4)
    return etfdata[(etfdata['Time']>starthour) & (etfdata['Time']<endhour)]

# We are gathering the etf prices for T-5 and T+5 minutes
# This data will be used for anlayzing signal strength
def analysePerformance(df=None, BuySellIndex=None):
    singalDf={}
    for dateindex in BuySellIndex.index:
        idx = df.index.get_loc(dateindex)
        resforward=df.iloc[(idx) : (idx + 6)]['ETF Change Price %']
        tempforward = list(resforward.values)

        resbackward=df.iloc[(idx-6) : (idx -1)]['ETF Change Price %']
        tempbackward = list(resbackward.values)
        
        if len(resforward.values)<6:
            [tempforward.append(np.nan) for i in range(5-len(resforward.values))]    
        
        if len(resbackward.values)<5:
            [tempbackward.append(np.nan) for i in range(5-len(resbackward.values))]    
        singalDf[dateindex] = tempbackward[::-1]+tempforward
    
    singalDf=pd.DataFrame.from_dict(singalDf, orient='index')
    singalDf.columns=['T-5','T-4','T-3','T-2','T-1','T','T+1','T+2','T+3','T+4','T+5']
    singalDf.loc['Total Return',:]=singalDf.sum(axis=0)
    return singalDf


def countRightSignals(data=None):
    resultDict={}
    buysignal=data[data['Over Bought/Sold']=='Buy']
    resultDict['# of Buy Signal']=buysignal.shape[0]
    resultDict['# of Right Buy Signal']=buysignal[buysignal['T+1']>0].shape[0]

    sellsignal=data[data['Over Bought/Sold']=='Sell']
    resultDict['# of Sell Signal']=sellsignal.shape[0]
    resultDict['# of Right Sell Signal']=sellsignal[sellsignal['T+1']<0].shape[0]

    return resultDict





