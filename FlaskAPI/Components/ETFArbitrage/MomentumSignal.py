import numpy as np
import talib

############################
# MOMENTUM SIGNALS
############################    
    
def MomentumSignals(df,tp=14):
    
    # SMA
    df['Momentum Signal']=talib.SMA(df['Close'],timeperiod=tp)
    description='SMA'
    df['Momentum Indicator'] = 0
    df.loc[(abs(df['ETF Price']) > df['Momentum Signal']), 'Momentum Indicator'] = 111
    df.loc[(abs(df['ETF Price']) < df['Momentum Signal']), 'Momentum Indicator'] = -111

    # CMO - Chande Momentum Oscillator
    df['CMO Signal']=talib.CMO(df['Close'],timeperiod=tp)
    description='CMO'
    df['CMO Indicator'] = 0
    df.loc[df['CMO Signal']>=50, 'CMO Indicator'] = 111
    df.loc[df['CMO Signal']<=-50, 'CMO Indicator'] = -111
    
    # ADX
    df['ADX Signal']=talib.ADX(df['High'],df['Low'],df['Close'],timeperiod=tp)
    description='ADX'
    df['ADX Trend'] = 0
    df.loc[df['ADX Signal']<25, 'ADX Trend'] = 'No Trend'
    df.loc[df['ADX Signal']>=25, 'ADX Trend'] = 'Weak Trend'
    df.loc[df['ADX Signal']>=50, 'ADX Trend'] = 'Strong Trend'
    df.loc[df['ADX Signal']>=75, 'ADX Trend'] = 'Extreme Strong Trend'

    # AROONOSC
    df['AROONOSC Signal']=talib.AROONOSC(df['High'],df['Low'],timeperiod=tp)
    df['AROONOSC Indicator'] = 0
    df.loc[df['AROONOSC Signal'] > 50, 'AROONOSC Trend'] = 'Uptrend'
    df.loc[df['AROONOSC Signal'] > 75, 'AROONOSC Trend'] = 'Strong Uptrend'
    df.loc[df['AROONOSC Signal'] < -50, 'AROONOSC Trend'] = 'Downtrend'
    df.loc[df['AROONOSC Signal'] < -75, 'AROONOSC Trend'] = 'Strong Downtrend'

    # RSI
    df['RSI Signal']=talib.RSI(df['Close'],timeperiod=tp)
    df['RSI Indicator'] = 0
    df.loc[df['RSI Signal']>=75, 'RSI Indicator'] = 111
    df.loc[df['RSI Signal']<=25, 'RSI Indicator'] = -111
    
    # ULTOSC - Ultimate Oscillator
    df['ULTOC Signal']=talib.ULTOSC(df['High'],df['Low'],df['Close'],timeperiod1=7, timeperiod2=14, timeperiod3=28)
    df['ULTOC Indicator'] = 0
    df.loc[df['ULTOC Signal']>=75, 'ULTOC Indicator'] = 111
    df.loc[df['ULTOC Signal']<=25, 'ULTOC Indicator'] = -111
    
    # WILLR  
    df['WILLR Signal']=talib.WILLR(df['High'],df['Low'],df['Close'],timeperiod=tp)
    df['WILLR Indicator'] = 0
    df.loc[df['WILLR Signal']>=-20, 'WILLR Indicator'] = 111
    df.loc[df['WILLR Signal']<=-80, 'WILLR Indicator'] = -111
    
    # MFI - Monet Flow Index
    df['MFI Signal']=talib.MFI(df['High'],df['Low'],df['Close'],df['Volume'],timeperiod=tp)
    df['MFI Indicator'] = 0
    df.loc[df['MFI Signal']>=75, 'MFI Indicator'] = 111
    df.loc[df['MFI Signal']<=25, 'MFI Indicator'] = -111
    
    # STOCHRSI
    fastk, fastd=talib.STOCHRSI(df['ETF Price'], timeperiod=tp, fastk_period=5, fastd_period=3, fastd_matype=0)
    df['FastStochastic']=fastk
    df['Stochastic Indicator']=np.where(fastk>80, 111, 0)+np.where(fastk<20, -111, 0)
    
    return df