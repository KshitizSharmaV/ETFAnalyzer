from pymongo import MongoClient
import pandas as pd
import datetime
import time
import numpy as np

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
    singalDf = {}
    for dateindex in BuySellIndex.index:
        idx = df.index.get_loc(dateindex)
        resforward = df.iloc[(idx): (idx + 2)]['ETF Change Price %']
        tempforward = list(resforward.values)

        if len(resforward.values) < 2:
            [tempforward.append(np.nan) for i in range(1 - len(resforward.values))]
        singalDf[dateindex] = tempforward

    singalDf = pd.DataFrame.from_dict(singalDf, orient='index')
    singalDf.columns = ['T', 'T+1']
    singalDf.loc['Total Return', :] = singalDf.sum(axis=0)
    return singalDf


def countRightSignals(data=None):
    resultDict={}
    buysignal=data[data['Over Bought/Sold']=='Over Sold']
    resultDict['# of Buy Signal']=buysignal.shape[0]
    resultDict['# of Right Buy Signal']=buysignal[buysignal['T+1']>0].shape[0]

    sellsignal=data[data['Over Bought/Sold']=='Over Bought']
    resultDict['# of Sell Signal']=sellsignal.shape[0]
    resultDict['# of Right Sell Signal']=sellsignal[sellsignal['T+1']<0].shape[0]

    return resultDict


etmoverslist = ['ETFMover%1', 'ETFMover%2', 'ETFMover%3', 'ETFMover%4', 'ETFMover%5',
                'ETFMover%6', 'ETFMover%7', 'ETFMover%8', 'ETFMover%9', 'ETFMover%10',
                'Change%1', 'Change%2', 'Change%3', 'Change%4', 'Change%5', 'Change%6',
                'Change%7', 'Change%8', 'Change%9', 'Change%10']
def etfMoversChangers(data):
    ########### Code to modify the ETF Movers and Underlying with highest change %
    # Seperate ETF Movers and the percentage of movement
    for movers in etmoverslist:
        def getTickerReturnFromMovers(x):
            # x = ast.literal_eval(x)
            return x[0], float(x[1])

        newcolnames = [movers + '_ticker', movers + '_value']
        data[movers] = data[movers].apply(getTickerReturnFromMovers)
        data[newcolnames] = pd.DataFrame(data[movers].tolist(), index=data.index)
        del data[movers]

    etfmoversList = dict(data[['ETFMover%1_ticker', 'ETFMover%2_ticker', 'ETFMover%3_ticker']].stack().value_counts())
    etfmoversDictCount = pd.DataFrame.from_dict(etfmoversList, orient='index', columns=['Count']).to_dict('index')

    highestChangeList = dict(data[['Change%1_ticker', 'Change%2_ticker', 'Change%3_ticker']].stack().value_counts())
    highestChangeDictCount = pd.DataFrame.from_dict(highestChangeList, orient='index', columns=['Count']).to_dict(
        'index')
    return etfmoversDictCount, highestChangeDictCount
    ########## Code to modify the ETF Movers and Underlying with highest change % ######



