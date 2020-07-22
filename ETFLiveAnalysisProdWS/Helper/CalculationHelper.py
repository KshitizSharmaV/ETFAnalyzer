''' Secondary Helper Functions needed for Live Arbitrage Calculation'''
import datetime
import traceback
import logging
import os

path = os.path.join(os.getcwd(), "Logs/")
if not os.path.exists(path):
    os.makedirs(path)
filename = path + datetime.datetime.now().strftime("%Y%m%d") + "-ArbPerMinLog.log"
handler = logging.FileHandler(filename)
logging.basicConfig(filename=filename, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', filemode='a')
logger = logging.getLogger("ArbPerMinLogger")
logger.setLevel(logging.DEBUG)
logger.addHandler(handler)
from MongoDB.Schemas import trade_per_min_WS


class tradestruct(): # For Trade Objects, containing current minute and last minute price for Tickers
    def calc_pct_chg(self, priceT, priceT_1):
        if priceT_1 == 0:
            return 0
        return ((priceT - priceT_1) / priceT_1) * 100

    def __init__(self, symbol, priceT, priceT_1=None):
        self.symbol = symbol
        self.priceT = priceT
        if not priceT_1:
            self.priceT_1 = priceT
        else:
            self.priceT_1 = priceT_1
        self.price_pct_chg = self.calc_pct_chg(self.priceT, self.priceT_1)

class LiveHelper():    
    def update_trade_dict(self, trade_dict, symbol, price, x):
        if symbol in trade_dict.keys():  # If trade object of said ETF/Holding is present in trade dict
            priceT_1 = trade_dict[symbol].priceT
            if x:  # Same 'x' as the one at call of this function. Serves as a flag here
                trade_obj = tradestruct(symbol=symbol, priceT=priceT_1, priceT_1=priceT_1)
            else:
                trade_obj = tradestruct(symbol=symbol, priceT=price, priceT_1=priceT_1)
            trade_dict[symbol] = trade_obj
        else:  # If trade object of said ETF/Holding is absent from trade dict
            trade_obj = tradestruct(symbol=symbol, priceT=price)
            trade_dict[symbol] = trade_obj
        return trade_dict
    
    def fetch_price_for_unrcvd_etfs(self, etflist, ticker):
        try:
            if ticker in etflist:  # Extract and store prev days price with today's timestamp only for ETFs and not Holdings
                dt_query = datetime.datetime.now().replace(second=0, microsecond=0)
                dt_query_ts = int(dt_query.timestamp() * 1000)
                last_recvd_data_for_ticker = trade_per_min_WS.find(
                    {"e": {"$lte": dt_query_ts}, "sym": ticker}).sort("e", -1).limit(1)
                x = [{legend: (
                    item[legend] if legend in item.keys() and legend in ['ev', 'sym', 'v', 'av', 'op', 'vw',
                                                                         'o', 'c', 'h', 'l', 'a'] else (
                        dt_query_ts - 60000 if legend == 's' else dt_query_ts)) for legend in
                    ['ev', 'sym', 'v', 'av', 'op', 'vw', 'o', 'c', 'h', 'l', 'a', 's', 'e']} for item in
                    last_recvd_data_for_ticker]
                return x
        except Exception as e:
            print("Exception in CalculatePerMinArb.py at line 84")
            print(e)
            traceback.print_exc()
            logger.exception(e)
    
    
    def get_top_movers_and_changes(self, tradedf, navdf, holdingsdf):
        abs_navdf = navdf.abs().sort_values(ascending=False)
        changedf = tradedf.loc[holdingsdf.index]
        abs_changedf = changedf['price_pct_chg'].abs().sort_values(ascending=False)
    
        if len(navdf) >= 10:
            moverdict = navdf.loc[abs_navdf.index][:10].to_dict()
            changedict = abs_changedf[:10].to_dict()
        else:
            moverdict = navdf.loc[abs_navdf.index][:].to_dict()
            changedict = abs_changedf[:].to_dict()
    
        moverdictlist = {}
        [moverdictlist.update({'ETFMover%' + str(i + 1): [item, moverdict[item]]}) for item, i in
         zip(moverdict, range(len(moverdict)))]
        changedictlist = {}
        [changedictlist.update({'Change%' + str(i + 1): [item, changedict[item]]}) for item, i in
         zip(changedict, range(len(changedict)))]
        return moverdictlist, changedictlist
