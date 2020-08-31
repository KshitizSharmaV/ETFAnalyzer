import numpy as np
from datetime import datetime
import sys
from FlaskAPI.Components.ETFArbitrage.ETFArbitrageMain import RetrieveETFArbitrageData
import datetime
import pandas as pd
import traceback


class AnalyzeCandlestickSignals():
    def __init__(self):
        self.MarketShouldUp = ['MorningStar Pat', 'Hammer Pat', 'InvertedHammer Pat', 'DragonFlyDoji Pat',
                               'PiercingLine Pat', '3WhiteSoldiers Pat']
        self.MarketShouldDown = ['HanginMan Pat', 'Shooting Pat']
        self.all_patterns = ['MorningStar Pat', 'Hammer Pat', 'InvertedHammer Pat', 'DragonFlyDoji Pat',
                             'PiercingLine Pat', '3WhiteSoldiers Pat', 'HanginMan Pat', 'Shooting Pat']

    def check_previous_3_returns(self, analyzeSignals, is_negative=True):
        betterSignal = []
        for i in range(0, len(analyzeSignals)):
            l = list(analyzeSignals['ETF Change Price %'][(i - 2):i])
            if is_negative:
                betterSignal.append(False) if len(l) == 0 else betterSignal.append(all(i < 0 for i in l))
            else:
                betterSignal.append(False) if len(l) == 0 else betterSignal.append(all(i > 0 for i in l))
        analyzeSignals['CustomSignal'] = betterSignal
        return analyzeSignals

    def AnalyzeKindOfSignal(self, etfdata, PatternName=None, valueForField=None):
        etf_name = etfdata['ETFName'][0]
        columns_needed = ['Time', 'ETF Trading Spread in $', 'Arbitrage in $', 'Magnitude of Arbitrage',
                          'Over Bought/Sold',
                          'ETF Price', 'ETF Change Price %'] + [PatternName]
        analyze_signals = etfdata[etfdata['ETFName'] == etf_name]

        if PatternName in self.MarketShouldUp and PatternName not in ['3WhiteSoldiers Pat']:
            analyze_signals = self.check_previous_3_returns(analyze_signals, is_negative=True)
            analyze_signals = analyze_signals[analyze_signals['CustomSignal'] == True]
        elif PatternName in self.MarketShouldDown:
            analyze_signals = self.check_previous_3_returns(analyze_signals, is_negative=False)
            analyze_signals = analyze_signals[analyze_signals['CustomSignal'] == True]

        analyze_signals = analyze_signals[analyze_signals[PatternName] == valueForField][columns_needed]
        # abs_of_arb = [0.01, 0.02, 0.03, 0.04, 0.05, 0.06, 0.07, 0.08, 0.09, 0.1, 100]
        return analyze_signals

    def analyze_etf_for_all_patterns(self, etfdata):
        signal_dfs = {}
        for PatternName in self.all_patterns:
            valueForField = list(set(etfdata[PatternName]))[-1]
            print(f"value for {PatternName} is {valueForField} for {etfdata['ETFName'][0]}")
            if valueForField == 0:
                continue
            pattern_signal = self.AnalyzeKindOfSignal(etfdata, PatternName=PatternName, valueForField=valueForField)
            top = pattern_signal.sort_values('Time', ascending=False).head(1)
            if not top.empty:
                recommendation = 'Buy' if PatternName in self.MarketShouldUp else (
                    'Sell' if PatternName in self.MarketShouldDown else 'Hold')
                signal_dfs[PatternName] = (top['Time'].values[0], recommendation)
        return signal_dfs
