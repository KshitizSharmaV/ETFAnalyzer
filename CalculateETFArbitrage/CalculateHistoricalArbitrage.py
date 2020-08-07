import sys  # Remove in production - KTZ

sys.path.append("..")  # Remove in production - KTZ

import pandas as pd
import datetime
import numpy as np

from PolygonTickData.Helper import Helper
from CalculateETFArbitrage.Helpers.LoadEtfHoldings import LoadHoldingsdata
from CalculateETFArbitrage.GatherAllData import DataApi


class ArbitrageCalculation():
    def __init__(self):
        self.helper_obj = Helper()

    def fetch_all_data(self, etf_name, date):
        """Load the ETF Holding"""
        etf_data = LoadHoldingsdata().LoadHoldingsAndClean(etfname=etf_name, fundholdingsdate=date)
        '''Load all the data - Holdings data for Etf, trade data, quotes data, open-close price'''
        all_data = DataApi(etfname=etf_name, date=date, etfData=etf_data)
        all_data.run_all_data_ops()
        return etf_data, all_data

    def trades_data_formatting_cleaning(self, all_data):
        all_data.tradesDataDf['Time'] = all_data.tradesDataDf['Time'].apply(
            lambda x: self.helper_obj.getHumanTime(ts=x, divideby=1000))
        trade_prices_df_minutes = \
            all_data.tradesDataDf.groupby([all_data.tradesDataDf['Time'], all_data.tradesDataDf['Symbol']])[
                'Trade Price']
        trade_prices_df_minutes = trade_prices_df_minutes.first().unstack(level=1)

        price_for_nav_filling = all_data.openPriceData.set_index('Symbol').T.to_dict('records')[0]

        market_start_time = datetime.datetime.strptime('13:29:00', "%H:%M:%S").time()
        market_end_time = datetime.datetime.strptime('20:00:00', "%H:%M:%S").time()

        mask = (trade_prices_df_minutes.index.time >= market_start_time) & (
                trade_prices_df_minutes.index.time <= market_end_time)
        trade_prices_df_minutes = trade_prices_df_minutes[mask]
        trade_prices_df_minutes = trade_prices_df_minutes.ffill(axis=0)
        trade_prices_df_minutes = trade_prices_df_minutes.fillna(price_for_nav_filling)
        return trade_prices_df_minutes

    def calculate_etf_price_change(self, trade_prices_df_minutes, etf_name):
        etf_price = trade_prices_df_minutes[etf_name]
        trade_prices_df_minutes = trade_prices_df_minutes.pct_change().dropna() * 100
        etf_price_change = trade_prices_df_minutes[etf_name]
        return etf_price, etf_price_change

    def calculate_etf_trading_spread(self, all_data):
        """Trading Spread Calculation"""
        all_data.quotesDataDf['Time'] = all_data.quotesDataDf['Time'].apply(
            lambda x: self.helper_obj.getHumanTime(ts=x, divideby=1000000000))
        all_data.quotesDataDf['Time'] = all_data.quotesDataDf['Time'].map(lambda x: x.replace(second=0))
        all_data.quotesDataDf = all_data.quotesDataDf[all_data.quotesDataDf['Bid Size'] != 0]
        all_data.quotesDataDf = all_data.quotesDataDf[all_data.quotesDataDf['Ask Size'] != 0]
        all_data.quotesDataDf['Total Bid Ask Size'] = all_data.quotesDataDf['Ask Size'] + all_data.quotesDataDf[
            'Bid Size']
        all_data.quotesDataDf['Spread'] = all_data.quotesDataDf['Ask Price'] - all_data.quotesDataDf['Bid Price']
        all_data.quotesDataDf['MidPrice'] = (all_data.quotesDataDf['Ask Price'] + all_data.quotesDataDf[
            'Bid Price']) / 2
        quotes_spreads_minutes = all_data.quotesDataDf.groupby('Time')['Spread'].mean()
        return quotes_spreads_minutes

    def calculateArbitrage(self, etf_name, date):
        """CALCULATE ETF ARBITRAGE FOR THE DATE"""
        etf_data, all_data = self.fetch_all_data(etf_name, date)
        '''Check if any holdings is trading in Non-US markets'''
        if all_data.openPriceData is None:
            return None
        '''Trade Prices formatting between 13.29 and 20.00'''
        trade_prices_df_minutes = self.trades_data_formatting_cleaning(all_data)
        '''Calculate Change in ETF Trade Price'''
        etf_price, etf_price_change = self.calculate_etf_price_change(trade_prices_df_minutes, etf_name)
        del trade_prices_df_minutes[etf_name]
        '''Trading Spread Calculation'''
        quotes_spreads_minutes = self.calculate_etf_trading_spread(all_data)
        '''Arbitrage Calculation'''
        net_asset_value_return = trade_prices_df_minutes.assign(**etf_data.getETFWeights()).mul(
            trade_prices_df_minutes).sum(
            axis=1)
        ds = pd.concat([etf_price, etf_price_change, net_asset_value_return, quotes_spreads_minutes], axis=1).dropna()
        ds.columns = ['ETF Price', 'ETF Change Price %', 'Net Asset Value Change%', 'ETF Trading Spread in $']
        ds['Arbitrage in $'] = (ds['ETF Change Price %'] - ds['Net Asset Value Change%']) * ds['ETF Price'] / 100
        ds['Flag'] = 0
        ds.loc[(abs(ds['Arbitrage in $']) > ds['ETF Trading Spread in $']) & ds[
            'ETF Trading Spread in $'] != 0, 'Flag'] = 111
        ds['Flag'] = ds['Flag'] * np.sign(ds['Arbitrage in $'])
        ''' Movers and Changes '''
        holdings_change = self.helper_obj.EtfMover(df=trade_prices_df_minutes, columnName='Change%')
        etf_mover_holdings = self.helper_obj.EtfMover(
            df=trade_prices_df_minutes.assign(**etf_data.getETFWeights()).mul(trade_prices_df_minutes).dropna(axis=1),
            columnName='ETFMover%')
        '''Final Calculated Result DataFrame'''
        ds = pd.concat([ds, etf_mover_holdings, holdings_change], axis=1)
        print(ds)
        return ds
