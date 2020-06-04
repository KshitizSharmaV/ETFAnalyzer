import talib

############################
# CANDLE PATTERNS RECOGNITION
############################    
def PatternSignals(df):

    def addCandle(functionname):
        return functionname(df['Open'],df['High'],df['Low'],df['Close']).replace({100:-111,-100:111})
    
    # Bullish Candles
    df['Hammer Pat'] = addCandle(talib.CDLHAMMER)
    df['InvertedHammer Pat']=addCandle(talib.CDLINVERTEDHAMMER)
    df['DragonFlyDoji Pat']=addCandle(talib.CDLDRAGONFLYDOJI)
    df['PiercingLine Pat']=addCandle(talib.CDLPIERCING)
    df['MorningStar Pat']=addCandle(talib.CDLMORNINGSTAR)
    df['MorningStarDoji Pat']=addCandle(talib.CDLMORNINGDOJISTAR)
    df['3WhiteSoldiers Pat']=addCandle(talib.CDL3WHITESOLDIERS)

    
    # Bearish Candles
    df['HanginMan Pat']=addCandle(talib.CDLHANGINGMAN)
    df['Shooting Pat']=addCandle(talib.CDLSHOOTINGSTAR)
    df['GraveStone Pat']=addCandle(talib.CDLGRAVESTONEDOJI)
    df['DarkCloud Pat']=addCandle(talib.CDLDARKCLOUDCOVER)
    df['EveningStar Pat']=addCandle(talib.CDLEVENINGSTAR)
    df['EveningDoji Pat']=addCandle(talib.CDLEVENINGDOJISTAR)
    df['3BlackCrows Pat']=addCandle(talib.CDL3BLACKCROWS)
    df['AbandonedBaby Pat']=addCandle(talib.CDLABANDONEDBABY)
    
    # Common Patterns
    df['Engulfing Pat']=addCandle(talib.CDLENGULFING)
    df['Harami Pat']=addCandle(talib.CDLHARAMI)
    
    # Indecission Patterns
    df['IndecisionSpinningTop Pat']=addCandle(talib.CDLSPINNINGTOP)
    df['IndecisionDoji Pat']=addCandle(talib.CDLDOJI)
    df['3LineStrike Pat']=addCandle(talib.CDL3LINESTRIKE)
    
    return df
