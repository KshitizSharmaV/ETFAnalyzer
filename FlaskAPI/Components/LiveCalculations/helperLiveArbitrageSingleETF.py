import pandas as pd
import traceback
from PolygonTickData.Helper import Helper
from datetime import datetime, timedelta, date
import numpy as np
import sys
import numpy as np

from FlaskAPI.Components.ETFArbitrage.ETFArbitrageMain import calculateArbitrageResults

daylightSavingAdjutment = 4 if date(2020,3,8)< datetime.now().date()< date(2020,11,1) else 5

# Fetch Arbitrage & Price Data
def fecthArbitrageANDLivePrices(etfname=None, FuncETFPrices=None, FuncArbitrageData=None, callAllDayArbitrage=None):
    try:
        # Full day historical Prie for ETF
        PriceDF = FuncETFPrices(etfname)
        PriceDF['Price'] = (PriceDF['high']+PriceDF['low'])/2
        # Full day historical Arbitrage for ETF
        ArbitrageDFSemi = FuncArbitrageData(etfname=etfname)
        
        ArbitrageDf = ArbitrageDFSemi.merge(PriceDF, left_on='Timestamp',right_on='date', how='left')
        ArbitrageDf =ArbitrageDf[['symbol','Timestamp','Arbitrage in $','ETF Trading Spread in $','Price','TickVolume','Net Asset Value Change%','ETF Change Price %']]
        ArbitrageDf=ArbitrageDf.round(5)
        
        helperObj=Helper()
        PriceDF['date'] = PriceDF['date'].apply(lambda x: helperObj.getHumanTime(ts=x, divideby=1000)-timedelta(hours=daylightSavingAdjutment))
        ArbitrageDf['Timestamp'] = ArbitrageDf['Timestamp'].apply(lambda x: str((helperObj.getHumanTime(ts=x, divideby=1000)-timedelta(hours=daylightSavingAdjutment)).time()))
        
        ArbitrageDf.rename(columns={'Timestamp':'Time'}, inplace=True)

        res={}
        res['Prices']=PriceDF[::-1]
        
        # All day analysis
        if callAllDayArbitrage:
            arbitrageBuySellSignals, pnlstatementforday, scatterPlotData=calculateArbitrageResults(df=ArbitrageDf, 
            etfname=etfname, 
            magnitudeOfArbitrageToFilterOn=0,
            BuildMomentumSignals=False, 
            BuildPatternSignals=False,
            includeMovers=False,
            getScatterPlot=False)
            
            cols_to_use = ArbitrageDf.columns.difference(arbitrageBuySellSignals.columns)
            arbitrageBuySellSignals['Time']=arbitrageBuySellSignals.index
            arbitrageBuySellSignals = arbitrageBuySellSignals.merge(ArbitrageDf[cols_to_use], left_on='Time',right_on='Time', how='left')
            
            del arbitrageBuySellSignals['ETF Change Price %']
            arbitrageBuySellSignals.rename(columns={'T':'ETF Change Price %'}, inplace=True)
            
            
            ##arbitrageBuySellSignals['ETF Change Price %']=np.round(arbitrageBuySellSignals['ETF Change Price %'],2)
            #arbitrageBuySellSignals['Price'] = np.round(arbitrageBuySellSignals['Price'],2)
            
            arbitrageBuySellSignals.Price=arbitrageBuySellSignals.Price.round(2)
            arbitrageBuySellSignals['ETF Change Price %']=arbitrageBuySellSignals['ETF Change Price %'].round(2)

            arbitrageBuySellSignals=arbitrageBuySellSignals
            
            res['Arbitrage'] = arbitrageBuySellSignals
            res['pnlstatementforday'] = pnlstatementforday
        else:
            res['Arbitrage']=ArbitrageDf
        
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
    ArbitrageDf.rename(columns={'Timestamp':'Time'}, inplace=True)
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


def CategorizeSignals(ArbitrageDf=None, ArbitrageColumnName=None, PriceColumn=None,Pct_change=None):
    ArbitrageDf=ArbitrageDf[::-1]
    ArbitrageDf=ArbitrageDf.reset_index()
    ArbitrageDf['AbsArbitrage']=abs(ArbitrageDf[ArbitrageColumnName])
    if Pct_change:
        ArbitrageDf['ETF Change Price %']=ArbitrageDf[PriceColumn].pct_change()*100
    else:
        ArbitrageDf['ETF Change Price %']=ArbitrageDf[PriceColumn]

    bins = [-0.1, 0.05, 0.10, 0.15, 0.20, np.inf]
    names = ['<0.05', '<0.10', '<0.15', '<0.20', '>0.20']
    ArbitrageDf['Group'] = pd.cut(ArbitrageDf['AbsArbitrage'], bins, labels=names)
    
    SignalCategorization = {
    '<0.05':{'# Buy Sign':0,'Buy Ret':0,'# Sell Sign':0,'Sell Ret':0},
    '<0.10':{'# Buy Sign':0,'Buy Ret':0,'# Sell Sign':0,'Sell Ret':0},
    '<0.15':{'# Buy Sign':0,'Buy Ret':0,'# Sell Sign':0,'Sell Ret':0},
    '<0.20':{'# Buy Sign':0,'Buy Ret':0,'# Sell Sign':0,'Sell Ret':0},
    '>0.20':{'# Buy Sign':0,'Buy Ret':0,'# Sell Sign':0,'Sell Ret':0}
    }
    
    lastindex=ArbitrageDf.index[-1]

    for idx in ArbitrageDf.index:
        if lastindex!=idx:
            groupType=ArbitrageDf.loc[idx,'Group']
            
            if ArbitrageDf.loc[idx,ArbitrageColumnName]<0:
                SignalCategorization[groupType]['# Buy Sign'] = SignalCategorization[groupType]['# Buy Sign'] + 1
                SignalCategorization[groupType]['Buy Ret'] = SignalCategorization[groupType]['Buy Ret'] + ArbitrageDf.loc[idx+1,'ETF Change Price %']
            elif ArbitrageDf.loc[idx,ArbitrageColumnName]>0:
                SignalCategorization[groupType]['# Sell Sign'] = SignalCategorization[groupType]['# Sell Sign'] + 1
                SignalCategorization[groupType]['Sell Ret'] = SignalCategorization[groupType]['Sell Ret'] + ArbitrageDf.loc[idx+1,'ETF Change Price %']

    SignalCategorization=pd.DataFrame(SignalCategorization).fillna(0).round(3).to_dict()
    SignalCategorization = {k: SignalCategorization[k] for k in names}
    return SignalCategorization

        
        
    
    











