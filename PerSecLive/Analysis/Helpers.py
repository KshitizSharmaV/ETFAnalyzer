import getpass
import os
import pathlib
import traceback
import sys

sys.path.append('..')
import pandas as pd
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
from FlaskAPI.Components.ETFArbitrage.ETFArbitrageMain import RetrieveETFArbitrageData
from MongoDB.MongoDBConnections import MongoDBConnectors


def visualizedata(df):
    plt.title("Histogram plot for arbitrage in $")
    plt.hist(df['Arbitrage in $'], bins=50)
    plt.show()

    plt.plot(df['Arbitrage in $'])
    plt.title("Arbitrage in $")
    plt.show()

    plt.plot(df['Price'])
    plt.title("Price Chart")
    plt.show()

    plt.title("Scatter Plot for Arbitrage in $ & T")
    plt.scatter(df['Arbitrage in $'], df['T'])
    plt.xlabel('Scatter Plot for Arbitrage in $')
    plt.ylabel('T')
    plt.show()

    plt.title("Scatter Plot for Arbitrage in $ & T+1")
    plt.scatter(df['Arbitrage in $'], df['T+1'])
    plt.xlabel('Scatter Plot for Arbitrage in $')
    plt.ylabel('T+1')
    plt.show()


def getETFArbitrageData(ETFName=None, listOfDates=None):
    AllDatesData = []
    print("***************************")
    print("Fetching data for etf = " + str(ETFName))
    for date in listOfDates:
        try:
            allData, pricedf, pnlstatementforday, scatterPlotData = RetrieveETFArbitrageData(etfname=ETFName, date=date,
                                                                                             magnitudeOfArbitrageToFilterOn=0)
            allData['ETFName'] = ETFName
            AllDatesData.append(allData)
        except Exception as ex:
            print(''.join(traceback.format_exception(etype=type(ex), value=ex, tb=ex.__traceback__)))
    AllDatesData = pd.concat(AllDatesData)
    return AllDatesData


# Find if wrong signals are 0 to 10 sec after right signals
def ifwrongsignalwasdueto_RightSignal(x, previousSignalTime):
    l = [(x - signal).total_seconds() for signal in previousSignalTime]
    m = min([i for i in l if (i > 0 and i < 30)], default=False)
    return True if m else m


def createResultDataFrame(resultdf, df, name):
    resultdf[name] = df.shape[0]
    resultdf["Return " + name] = df['T+1'].sum()
    return resultdf


def previousnreturnsWereNegative(analyzeSignals, n):
    betterSignal = []
    for i in range(0, len(analyzeSignals)):
        l = list(analyzeSignals['T'][(i - n):(i)])
        betterSignal.append(False) if len(l) == 0 else betterSignal.append(all(i < 0 for i in l))
    analyzeSignals['CustomSignal'] = betterSignal
    return analyzeSignals


def previousnreturnsWerePositive(analyzeSignals, n):
    betterSignal = []
    for i in range(0, len(analyzeSignals)):
        l = list(analyzeSignals['T'][(i - n):(i)])
        betterSignal.append(False) if len(l) == 0 else betterSignal.append(all(i > 0 for i in l))
    analyzeSignals['CustomSignal'] = betterSignal
    return analyzeSignals


def get_repo_root_path():
    rootpath = pathlib.Path(os.getcwd())
    while str(rootpath).split('/')[-1] != 'ETFAnalyzer':
        rootpath = rootpath.parent
    return rootpath


def make_arbitrage_csv_files(etfname, datelist):
    connection = MongoDBConnectors().get_pymongo_readonly_devlocal_production()
    collection = connection.ETF_db.PerSecLiveArbitrage
    out_dir = os.path.abspath(os.path.join(get_repo_root_path(),
                                           f'PerSecLive/{etfname}Data/'))
    if not os.path.exists(out_dir):
        os.mkdir(out_dir)
    for date in datelist:
        res = collection.find({'ETFName': etfname, 'End_Time': {'$gte': datetime.strptime(date, '%Y-%m-%d'),
                                                                '$lt': datetime.strptime(date, '%Y-%m-%d') + timedelta(
                                                                    days=1)}}, {'_id': 0, 'ETFName': 0})
        df = pd.DataFrame(list(res))
        df = df[::-1]
        y, m, d = date.split('-')
        df.to_csv(out_dir+f'/{etfname}-ArbitrageData{y}{m}{d}.csv')
        print(f'Saved csv for {etfname} for {y+m+d}')
