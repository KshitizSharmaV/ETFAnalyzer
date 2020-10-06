import sys
sys.path.append('..')
import numpy as np
import pandas as pnd
from PerSecLive.Analysis.Helpers import *


def AnalyzeSignalsData(analyzeSignals=None, currentMag=None, nextArbMag=None, includeSpread=None):
    # print("#####################")
    # print("Magnitude Of Arbitrage = " + str(currentMag) + ' < '+str(nextArbMag))

    if includeSpread:
        analyzeSignals = analyzeSignals[abs(analyzeSignals['Magnitude of Arbitrage']) > currentMag]
        analyzeSignals = analyzeSignals[abs(analyzeSignals['Magnitude of Arbitrage']) < nextArbMag]
    else:
        analyzeSignals = analyzeSignals[abs(analyzeSignals['Arbitrage in $']) > currentMag]
        analyzeSignals = analyzeSignals[abs(analyzeSignals['Arbitrage in $']) < nextArbMag]

    result = {}
    dataf = {}

    no_of_neg_arb = analyzeSignals[analyzeSignals['Arbitrage in $'] < 0]
    no_of_pos_arb = analyzeSignals[analyzeSignals['Arbitrage in $'] > 0]

    ##################
    # Right Signals
    ##################
    negativeArb_PosReturn = analyzeSignals[(analyzeSignals['Arbitrage in $'] < 0) & ((analyzeSignals['T+1'] > 0))]
    result['negativeArb_PosReturn'] = negativeArb_PosReturn
    posArb_NegReturn = analyzeSignals[(analyzeSignals['Arbitrage in $'] > 0) & ((analyzeSignals['T+1'] < 0))]
    result['posArb_NegReturn'] = posArb_NegReturn

    ##################
    # Wrong Signals
    ##################
    negativeArb_NegReturn = analyzeSignals[(analyzeSignals['Arbitrage in $'] < 0) & ((analyzeSignals['T+1'] < 0))]
    negativeArb_NegReturn['TrueSignal'] = negativeArb_NegReturn['Time'].apply(
        lambda x: ifwrongsignalwasdueto_RightSignal(x, list(posArb_NegReturn['Time'])))
    negativeArb_NegReturn = negativeArb_NegReturn[negativeArb_NegReturn['TrueSignal'] == False]

    posArb_posReturn = analyzeSignals[(analyzeSignals['Arbitrage in $'] > 0) & ((analyzeSignals['T+1'] > 0))]
    posArb_posReturn['TrueSignal'] = posArb_posReturn['Time'].apply(
        lambda x: ifwrongsignalwasdueto_RightSignal(x, list(negativeArb_PosReturn['Time'])))
    posArb_posReturn = posArb_posReturn[posArb_posReturn['TrueSignal'] == False]

    ##################
    # Signals but we have 0 return
    ##################
    negativeArb_ZeroReturn = analyzeSignals[(analyzeSignals['Arbitrage in $'] < 0) & ((analyzeSignals['T+1'] == 0))]
    posArb_zeroReturn = analyzeSignals[(analyzeSignals['Arbitrage in $'] > 0) & ((analyzeSignals['T+1'] == 0))]

    ##################
    # Create a data frame
    ##################
    dataf = createResultDataFrame(dataf, negativeArb_PosReturn, "Arb -ve & Return +ve")
    dataf = createResultDataFrame(dataf, negativeArb_NegReturn, "Arb -ve & Return -ve")
    dataf = createResultDataFrame(dataf, negativeArb_ZeroReturn, "Arb -ve & Return 0")
    dataf = createResultDataFrame(dataf, posArb_NegReturn, "Arb +ve & Return -ve")
    dataf = createResultDataFrame(dataf, posArb_posReturn, "Arb +ve & Return +ve")
    dataf = createResultDataFrame(dataf, posArb_zeroReturn, "Arb +ve & Return 0")
    dataf['Accuracy GoUp'] = dataf["Arb -ve & Return +ve"] / no_of_neg_arb.shape[0] if no_of_neg_arb.shape[
                                                                                           0] != 0 else np.nan
    dataf['Accuracy GoDown'] = dataf["Arb +ve & Return -ve"] / no_of_pos_arb.shape[0] if no_of_pos_arb.shape[
                                                                                             0] != 0 else np.nan

    result['dataf'] = dataf

    if currentMag == 0.1 and includeSpread == True:
        print("##### - Ve Arb, +ve Return")
        print(negativeArb_PosReturn)
        print("##### + Ve Arb, +ve Return")
        print(posArb_posReturn)

        print("##### + Ve Arb, -ve Return")
        print(posArb_NegReturn)
        print("##### - Ve Arb, -ve Return")
        print(negativeArb_NegReturn)

    return result



def AnalyzeETFPerformance(AllDatesData=None, ETFName=None, includeSpread=None):
    print(" Signals for " + ETFName + " includeSpread:" + str(includeSpread))
    analyzeSignals = AllDatesData[AllDatesData['ETFName'] == ETFName]

    # Add Logic to filter
    '''
    analyzeSignals = previousnreturnsWereNegative(analyzeSignals,2)
    analyzeSignals = previousnreturnsWerePositive(analyzeSignals,2)
    analyzeSignals=analyzeSignals[analyzeSignals['CustomSignal']==True]
    '''

    AbsOfArb = [0, 0.01, 0.02, 0.03, 0.04, 0.05, 0.06, 0.07, 0.08, 0.09, 0.1, 100]
    alletfDF = []
    rightDF = pnd.DataFrame()
    wrongDF = pnd.DataFrame()
    for magnitudeOfArbitrage_index in range(0, len(AbsOfArb) - 1):
        currentMag = AbsOfArb[magnitudeOfArbitrage_index]
        nextArbMag = AbsOfArb[magnitudeOfArbitrage_index + 1]
        result = AnalyzeSignalsData(analyzeSignals=analyzeSignals, currentMag=currentMag, nextArbMag=nextArbMag,
                                    includeSpread=includeSpread)
        dataf = result['dataf']
        dataf['Magnitude'] = str(currentMag) + ' < ' + str(nextArbMag)
        dataf['ETFName'] = ETFName
        alletfDF.append(dataf)
    return alletfDF


datafColumnsorder = ["Arb -ve & Return +ve", "Return Arb -ve & Return +ve",
                     "Arb -ve & Return -ve", "Return Arb -ve & Return -ve",
                     "Arb -ve & Return 0", "Return Arb -ve & Return 0",
                     "Accuracy GoUp",
                     "Arb +ve & Return -ve", "Return Arb +ve & Return -ve",
                     "Arb +ve & Return +ve", "Return Arb +ve & Return +ve",
                     "Arb +ve & Return 0", "Return Arb +ve & Return 0",
                     "Accuracy GoDown", "Magnitude", "ETFName"]


def runanalysis(filename, etfname):
    data = pnd.read_csv(filename, header=0)
    data = data[['End_Time', 'Arbitrage', 'ETFPrice', 'Spread']]
    data.rename(columns={'End_Time': 'Time', 'Arbitrage': 'Arbitrage in $', 'ETFPrice': 'Price'}, inplace=True)
    # data.columns = ['Time', 'Arbitrage in $', 'Price', 'Spread']
    data['Time'] = pnd.to_datetime(data['Time'], format='%Y-%m-%d %H:%M:%S')

    data['T'] = data['Price'].pct_change() * 100
    data['T+1'] = data['T'].shift(-1)
    data['T+2'] = data['T'].shift(-2)

    data['Magnitude of Arbitrage'] = abs(data['Arbitrage in $']) - data['Spread']
    # Replace all negative values with 0
    data['Magnitude of Arbitrage'] = data['Magnitude of Arbitrage'].mask(data['Magnitude of Arbitrage'].lt(0), 0)
    data['ETFName'] = etfname

    print(data.head())
    visualizedata(data)

    '''
    alletfDF=pnd.DataFrame(AnalyzeETFPerformance(AllDatesData=data, ETFName=etfname,includeSpread=False))
    alletfDF = alletfDF[datafColumnsorder]
    display(alletfDF.set_index('Magnitude'))
    '''

    alletfDF = pnd.DataFrame(AnalyzeETFPerformance(AllDatesData=data, ETFName=etfname, includeSpread=True))
    alletfDF = alletfDF[datafColumnsorder]
    print(alletfDF.set_index('Magnitude'))

    return data
