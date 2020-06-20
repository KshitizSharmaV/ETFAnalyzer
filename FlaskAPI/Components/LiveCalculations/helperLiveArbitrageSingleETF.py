import pandas as pd
import traceback
from PolygonTickData.Helper import Helper
from datetime import datetime, timedelta, date
import numpy as np
import sys

from FlaskAPI.Components.ETFArbitrage.ETFArbitrageMain import calculateArbitrageResults

daylightSavingAdjutment = 4 if date(2020,3,8)< datetime.now().date()< date(2020,11,1) else 5

# Fetch Arbitrage & Price Data
def fecthArbitrageANDLivePrices(etfname=None, FuncETFPrices=None, FuncArbitrageData=None):
    try:
        # Full day historical Prie for ETF
        PriceDF = FuncETFPrices(etfname)

        # Full day historical Arbitrage for ETF
        ArbitrageDF = FuncArbitrageData(etfname=etfname)
        mergedDF = ArbitrageDF.merge(PriceDF, left_on='Timestamp',right_on='date', how='left')
        mergedDF =mergedDF[['Symbol','Timestamp','Arbitrage','Spread','VWPrice','TickVolume','Net Asset Value Change%','ETF Change Price %']]
        mergedDF=mergedDF.round(5)
        
        helperObj=Helper()
        PriceDF['date'] = PriceDF['date'].apply(lambda x: helperObj.getHumanTime(ts=x, divideby=1000)-timedelta(hours=daylightSavingAdjutment))
        mergedDF['Timestamp'] = mergedDF['Timestamp'].apply(lambda x: str((helperObj.getHumanTime(ts=x, divideby=1000)-timedelta(hours=daylightSavingAdjutment)).time()))
        
        #res=jsonify(Full_Day_Prices=PriceDF[::-1].to_csv(sep='\t', index=False), Full_Day_Arbitrage_Data=mergedDF.to_dict())
        res={}
        res['Prices']=PriceDF[::-1]
        res['Arbitrage']=mergedDF
        return res

    except Exception as e:
        print("Issue in Flask app while fetching ETF Description Data")
        print(traceback.format_exc())
        return str(e)


# Signal Analyzer
def analyzeSignalPerformane(Arbitrage=None):
    SignalInfo={}
    # Check here for kind of ETF and adjust Sell or Buy Signal Accordingly
    if Arbitrage==0:
        SignalInfo['ETFStatus'] = 'Balanced'
        SignalInfo['Signal'] = 'Hold'
        SignalInfo['Strength'] = 'Weak'
        return SignalInfo

    elif Arbitrage<0:
        SignalInfo['ETFStatus'] = 'Over Sold'
        SignalInfo['Signal'] = 'Buy'

    else:
        SignalInfo['ETFStatus'] = 'Over Bought'
        SignalInfo['Signal'] = 'Sell'

    # Measurement of signal
    absoluteArbitrage = abs(Arbitrage)
    if absoluteArbitrage<0.05:
        SignalInfo['Strength'] = 'Weak'
    elif absoluteArbitrage<0.10:
        SignalInfo['Strength'] = 'Good'
    elif absoluteArbitrage<0.15:
        SignalInfo['Strength'] = 'Strong'
    elif absoluteArbitrage<0.20:
        SignalInfo['Strength'] = '+ Strong'
    else:
        SignalInfo['Strength'] = '++ Strong'

    return SignalInfo


def AnalyzeDaysPerformance(ArbitrageDf=None,etfname=None):
    ArbitrageDf=ArbitrageDf[::-1]
    ArbitrageDf.rename(columns={'Timestamp':'Time',
        'Spread':'ETF Trading Spread in $',
        'Arbitrage':'Arbitrage in $'}, inplace=True)
    ArbitrageDf['ETF Change Price %']=ArbitrageDf['VWPrice'].pct_change()*100
    
    ArbitrageDf=ArbitrageDf.dropna()
    arbitrageBuySellSignals, pnlstatementforday, scatterPlotData=calculateArbitrageResults(df=ArbitrageDf, 
        etfname=etfname, 
        magnitudeOfArbitrageToFilterOn=0,
        BuildMomentumSignals=False, 
        BuildPatternSignals=False,
        includeMovers=False,
        getScatterPlot=False)
    
    return pnlstatementforday



def CategorizeSignals(ArbitrageDf=None):
    ArbitrageDf=ArbitrageDf[::-1]
    ArbitrageDf=ArbitrageDf.reset_index()
    ArbitrageDf['AbsArbitrage']=abs(ArbitrageDf['Arbitrage'])
    ArbitrageDf['ETF Change Price %']=ArbitrageDf['VWPrice'].pct_change()*100

    bins = [-0.1, 0.05, 0.10, 0.15, 0.20, np.inf]
    names = ['Weak', 'Good', 'Strong', '+ Strong', '++ Strong']
    ArbitrageDf['Group'] = pd.cut(ArbitrageDf['AbsArbitrage'], bins, labels=names)
    
    SignalCategorization = {
    'Weak':{'# Buy Signals':0,'Buy Return':0,'# Sell Signals':0,'Sell Return':0},
    'Good':{'# Buy Signals':0,'Buy Return':0,'# Sell Signals':0,'Sell Return':0},
    'Strong':{'# Buy Signals':0,'Buy Return':0,'# Sell Signals':0,'Sell Return':0},
    '+ Strong':{'# Buy Signals':0,'Buy Return':0,'# Sell Signals':0,'Sell Return':0},
    '++ Strong':{'# Buy Signals':0,'Buy Return':0,'# Sell Signals':0,'Sell Return':0}
    }
    
    lastindex=ArbitrageDf.index[-1]

    for idx in ArbitrageDf.index:
        if lastindex!=idx:
            groupType=ArbitrageDf.loc[idx,'Group']
            
            if ArbitrageDf.loc[idx,'Arbitrage']<0:
                SignalCategorization[groupType]['# Buy Signals'] = SignalCategorization[groupType]['# Buy Signals'] + 1
                SignalCategorization[groupType]['Buy Return'] = SignalCategorization[groupType]['Buy Return'] + ArbitrageDf.loc[idx+1,'ETF Change Price %']
            elif ArbitrageDf.loc[idx,'Arbitrage']>0:
                SignalCategorization[groupType]['# Sell Signals'] = SignalCategorization[groupType]['# Sell Signals'] + 1
                SignalCategorization[groupType]['Sell Return'] = SignalCategorization[groupType]['Sell Return'] + ArbitrageDf.loc[idx+1,'ETF Change Price %']

    
    SignalCategorization=pd.DataFrame(SignalCategorization).fillna(0).round(3).to_dict()
    SignalCategorization = {k: SignalCategorization[k] for k in names}
    print("CategorizeSignals")
    print(SignalCategorization)
    return SignalCategorization

        
        
    
    











