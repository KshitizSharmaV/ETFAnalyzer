import datetime
from MongoDB.Schemas import trade_per_min_WS_motor, trade_per_min_WS, quotesWS_collection, arbitrage_per_min
import pandas as pd
from time import time
from CommonServices.Holidays import HolidayCheck,LastWorkingDay,isTimeBetween


class PerMinDataOperations():

    # Use AsyncIOMotorCursor for inserting into TradePerMinWS Collection
    async def do_insert(self, data):
        result = await trade_per_min_WS_motor.insert_many(data)
        print('inserted %d docs' % (len(result.inserted_ids),))

    # Insert into QuotesLiveData Collection
    def insertQuotesLive(self, quotesData):
        quotesWS_collection.insert_many(quotesData, ordered=False)

    # Use PyMongo Cursor for fetching from TradePerMinWS Collection
    def FetchAllTradeDataPerMin(self, startts, endts):
        all_tickers_data = trade_per_min_WS.find({'e': {'$gt': startts, '$lte': endts}}, {'_id': 0, 'sym': 1, 'vw': 1})
        return all_tickers_data

    # Fetch from QuotesLiveData Collection
    def FetchQuotesLiveDataForSpread(self, startts, endts):
        quotes_data_for_etf = quotesWS_collection.find({'timestamp': {'$gt': startts, '$lte': endts}},{'_id':0,'symbol': 1, 'askprice': 1, 'bidprice': 1})
        return quotes_data_for_etf


    #################################
    # Hostorical Arbitrage & Price for a day
    #################################

    # Fetch full day arbitrage for 1 etf
    def FetchFullDayPerMinArbitrage(self, etfname):
        
        if datetime.datetime.now().time() > datetime.time(9,15):
            print("Went Inside 1")
            day_start_dt = datetime.datetime.strptime(' '.join([str(datetime.datetime.now().date()), '09:15']),'%Y-%m-%d %H:%M')
            day_start_ts = int(day_start_dt.timestamp() * 1000)
            print("FetchFullDayPerMinArbitrage "+ str(day_start_ts))
            # Get data for today
            full_day_data_cursor = arbitrage_per_min.find(
            {"Timestamp": {"$gte": day_start_ts}, "ArbitrageData.symbol": etfname},
            {"_id": 0, "Timestamp": 1, "ArbitrageData.$": 1})
        else:
            lastworkinDay=LastWorkingDay(datetime.datetime.now().date())
            day_start_dt = datetime.datetime.strptime(' '.join([str(lastworkinDay.date()), '09:15']),'%Y-%m-%d %H:%M')
            day_start_ts = int(day_start_dt.timestamp() * 1000)
            print("FetchFullDayPerMinArbitrage"+ str(day_start_ts))
            day_end_dt = datetime.datetime.strptime(' '.join([str(lastworkinDay.date()), '03:59']),'%Y-%m-%d %H:%M')
            day_end_dt = int(day_end_dt.timestamp() * 1000)
            print("FetchFullDayPerMinArbitrage"+ str(day_end_dt))
            # Get last woring day data
            full_day_data_cursor = arbitrage_per_min.find(
            {"Timestamp": {"$gte": day_start_ts,"$lte": day_end_dt}, "ArbitrageData.symbol": etfname},
            {"_id": 0, "Timestamp": 1, "ArbitrageData.$": 1})

        data = []
        [data.append({'Timestamp': item['Timestamp'], 
                    'Symbol': item['ArbitrageData'][0]['symbol'],
                    'Arbitrage': item['ArbitrageData'][0]['Arbitrage in $'], 
                    'Spread': item['ArbitrageData'][0]['ETF Trading Spread in $'],
                    'ETF Change Price %': item['ArbitrageData'][0]['ETF Change Price %'],
                    'Net Asset Value Change%': item['ArbitrageData'][0]['Net Asset Value Change%']})
                    for item in full_day_data_cursor]
        full_day_data_df = pd.DataFrame.from_records(data)

        print(full_day_data_df)
        return full_day_data_df


    # Full full  day prices for 1 etf
    def FetchFullDayPricesForETF(self, etfname):
        
        if datetime.datetime.now().time() > datetime.time(9,15):
            print("Went Inside 1")
            day_start_dt = datetime.datetime.strptime(' '.join([str(datetime.datetime.now().date()), '09:15']),'%Y-%m-%d %H:%M')
            day_start_ts = int(day_start_dt.timestamp() * 1000)
            print("FetchFullDayPricesForETF"+ str(day_start_ts))
            # Get data for today
            full_day_prices_etf_cursor = trade_per_min_WS.find({"e": {"$gte": day_start_ts}, "sym": etfname},
                                        {"_id": 0, "sym": 1, "vw": 1,"o":1,"c":1,"h":1,"l":1,"v":1,"e": 1})
        else:
            lastworkinDay=LastWorkingDay(datetime.datetime.now().date())
            day_start_dt = datetime.datetime.strptime(' '.join([str(lastworkinDay.date()), '09:15']),'%Y-%m-%d %H:%M')
            day_start_ts = int(day_start_dt.timestamp() * 1000)
            print("FetchFullDayPricesForETF"+ str(day_start_ts))
            day_end_dt = datetime.datetime.strptime(' '.join([str(lastworkinDay.date()), '03:59']),'%Y-%m-%d %H:%M')
            day_end_dt = int(day_end_dt.timestamp() * 1000)
            print("FetchFullDayPricesForETF"+ str(day_end_dt))
            # Get last woring day data
            full_day_prices_etf_cursor = trade_per_min_WS.find({"e": {"$gte": day_start_ts,"$lte":day_end_dt}, "sym": etfname},
                                        {"_id": 0, "sym": 1, "vw": 1,"o":1,"c":1,"h":1,"l":1,"v":1,"e": 1})

        temp = []
        [temp.append(item) for item in full_day_prices_etf_cursor]
        livePrices = pd.DataFrame.from_records(temp)
        livePrices.rename(columns={'sym': 'Symbol', 'vw': 'VWPrice','o':'open','c':'close','h':'high','l':'low','v':'TickVolume', 'e': 'date'}, inplace=True)
        livePrices.drop(columns=['Symbol'], inplace=True)
        print("FetchFullDayPricesForETF")
        print(livePrices)

        return livePrices

    #################################
    # Live Arbitrage & Price for 1 or all ETF
    #################################

    #  Live arbitrage for 1 etf or all etf
    def LiveFetchPerMinArbitrage(self, etfname=None):
        currentTime=datetime.datetime.now().time()
        if (currentTime >= datetime.time(9,15)) and (currentTime <= datetime.time(15,59)):
            dt = (datetime.datetime.now()).replace(second=0, microsecond=0)
        elif (currentTime >= datetime.time(16,00)) and (currentTime <= datetime.time(23,59)):
            dt = (datetime.datetime.now()).replace(hour=15,minute=59,second=0, microsecond=0)
        elif (currentTime >= datetime.time(00,00)) and (currentTime <= datetime.time(9,14)):
            lastworkinDay=LastWorkingDay(datetime.datetime.now().date())
            dt = lastworkinDay.replace(hour=15,minute=59,second=0, microsecond=0)    

        dt_ts = int(dt.timestamp() * 1000)
        print("LiveFetchPerMinArbitrage "+str(dt_ts))
        
        # Data for 1 ticker
        data = []
        if etfname:
            live_per_min_cursor = arbitrage_per_min.find(
                {"Timestamp": dt_ts, "ArbitrageData.symbol": etfname},
                {"_id": 0, "Timestamp": 1, "ArbitrageData.$": 1})
            
            [data.append({'Timestamp': item['Timestamp'], 
                'Symbol': item['ArbitrageData'][0]['symbol'],
                'Arbitrage': item['ArbitrageData'][0]['Arbitrage in $'], 
                'Spread': item['ArbitrageData'][0]['ETF Trading Spread in $'],
                'ETF Change Price %': item['ArbitrageData'][0]['ETF Change Price %'],
                'Net Asset Value Change%': item['ArbitrageData'][0]['Net Asset Value Change%']
                })
                for item in live_per_min_cursor]
        else:
            # Data For Multiple Ticker for live minute
            live_per_min_cursor = arbitrage_per_min.find(
                {"Timestamp": dt_ts},
                {"_id": 0, "Timestamp": 1, "ArbitrageData": 1})
            [data.extend(item['ArbitrageData']) for item in live_per_min_cursor]
             
        liveArbitrageData_onemin = pd.DataFrame.from_records(data)
        print("liveArbitrageData_onemin")
        print(liveArbitrageData_onemin)
        return liveArbitrageData_onemin

    # LIVE 1 Min prices for 1 or all etf
    def LiveFetchETFPrice(self, etfname=None):
        currentTime=datetime.datetime.now().time()
        
        if (currentTime >= datetime.time(9,15)) and (currentTime <= datetime.time(15,59)):
            dt = (datetime.datetime.now()).replace(second=0, microsecond=0)
        elif (currentTime >= datetime.time(16,00)) and (currentTime <= datetime.time(23,59)):
            dt = (datetime.datetime.now()).replace(hour=15,minute=59,second=0, microsecond=0)
        elif (currentTime >= datetime.time(00,00)) and (currentTime <= datetime.time(9,14)):
            lastworkinDay=LastWorkingDay(datetime.datetime.now().date())
            dt = lastworkinDay.replace(hour=15,minute=59,second=0, microsecond=0)    
        
        dt_ts = int(dt.timestamp() * 1000)
        print("LiveFetchETFPrice "+str(dt_ts))
        if etfname:
            etf_live_prices_cursor = trade_per_min_WS.find({"e": dt_ts,"sym": etfname}, {"_id": 0, "sym": 1, "vw": 1,"o":1,"c":1,"h":1,"l":1,"v":1, "e": 1})
        else:
            etf_live_prices_cursor = trade_per_min_WS.find({"e": dt_ts}, {"_id": 0, "sym": 1, "vw": 1,"o":1,"c":1,"h":1,"l":1,"v":1, "e": 1})
        
        temp = []
        [temp.append(item) for item in etf_live_prices_cursor]
        livePrices = pd.DataFrame.from_records(temp)
        livePrices.rename(columns={'sym': 'Symbol', 'vw': 'VWPrice','o':'open','c':'close','h':'high','l':'low','v':'TickVolume', 'e': 'date'}, inplace=True)
        if etfname:
            livePrices.drop(columns=['Symbol'], inplace=True)
        print("LiveFetchETFPrice")
        print(livePrices)
        return livePrices

    
    
