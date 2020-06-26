import datetime
from MongoDB.Schemas import trade_per_min_WS_motor, trade_per_min_WS, quotesWS_collection, arbitrage_per_min
import pandas as pd
from time import time
from dateutil import tz
from CommonServices.Holidays import HolidayCheck,LastWorkingDay,isTimeBetween


class PerMinDataOperations():

    def __init__(self):
        ''' Day End Times in UTC'''
        self.DAYendTime = datetime.time(3,59) if datetime.date(2020,3,8)< datetime.datetime.now().date()< datetime.date(2020,11,1) else datetime.time(4,59)
        # self.DAYendTime = datetime.time(23,59)
        self.DAYendTimeZeroZeo = datetime.time(4,00) if datetime.date(2020,3,8)< datetime.datetime.now().date()< datetime.date(2020,11,1) else datetime.time(5,00)
        # self.DAYendTimeZeroZeo = datetime.time(0,0)

        # Day Light Savings
        # Summer UTC 13 to 20
        # Winter UTC 14 to 21
        self.daylightSavingAdjutment = 4 if datetime.date(2020,3,8)< datetime.datetime.now().date()< datetime.date(2020,11,1) else 5
        self.StartHour = 13 if datetime.date(2020,3,8)<datetime.datetime.now().date()<datetime.date(2020,11,1) else 14
        self.EndHour = 20 if datetime.date(2020,3,8)<datetime.datetime.now().date()<datetime.date(2020,11,1) else 21
        self.UTCStartTime =  datetime.time(self.StartHour,30)
        self.UTCEndTime =  datetime.time(self.EndHour,00)
        
        
    # Use AsyncIOMotorCursor for inserting into TradePerMinWS Collection
    async def do_insert(self, data):
        result = await trade_per_min_WS_motor.insert_many(data)
        print('inserted %d docs' % (len(result.inserted_ids),))

    # Insert into QuotesLiveData Collection
    def insertQuotesLive(self, quotesData):
        quotesWS_collection.insert_many(quotesData, ordered=False)


    # Use PyMongo Cursor for fetching from TradePerMinWS Collection
    def FetchAllTradeDataPerMin(self, startts, endts):
        all_tickers_data = trade_per_min_WS.find({'e': {'$gt': startts, '$lte': endts}}, {'_id': 0, 'sym': 1, 'h': 1, 'l':1})
        return all_tickers_data

    # Fetch from QuotesLiveData Collection
    def FetchQuotesLiveDataForSpread(self, startts, endts):
        quotes_data_for_etf = quotesWS_collection.find({'timestamp': {'$gt': startts, '$lte': endts}},{'_id':0,'symbol': 1, 'askprice': 1, 'bidprice': 1})
        return quotes_data_for_etf


    #################################
    # Hostorical Arbitrage & Price for a day
    #################################
    def getMarketConditionsForFullDayData(self):
        now =  datetime.datetime.utcnow()
        currentTime = now.time()
        todaysDate = now.date()
        ifaholiday = HolidayCheck(todaysDate)   
        end_dt=None
        if currentTime >= self.UTCStartTime and (not ifaholiday): 
            start_dt = now
            start_dt = start_dt.replace(hour=self.StartHour,minute=30,second=0, microsecond=0)
            # start_dt=start_dt - datetime.timedelta(hours=self.daylightSavingAdjutment)
            start_dt = start_dt.replace(tzinfo = tz.gettz('UTC'))
            start_dt = start_dt.astimezone(tz.tzlocal())
        else:
            lastworkinDay=LastWorkingDay(todaysDate)
            start_dt = lastworkinDay

            start_dt = start_dt.replace(hour=self.StartHour,minute=30,second=0, microsecond=0)
            start_dt = start_dt.replace(tzinfo=tz.gettz('UTC'))
            start_dt = start_dt.astimezone(tz.tzlocal())

            end_dt =  lastworkinDay.replace(hour=self.EndHour,minute=00,second=0, microsecond=0)
            end_dt = end_dt.replace(tzinfo=tz.gettz('UTC'))
            end_dt = end_dt.astimezone(tz.tzlocal())
            #end_dt=end_dt - datetime.timedelta(hours=self.daylightSavingAdjutment)
        
        #start_dt = start_dt.replace(hour=self.StartHour,minute=30,second=0, microsecond=0)
        # Fix for breaking code
        #start_dt=start_dt - datetime.timedelta(hours=self.daylightSavingAdjutment)
        
        FetchDataForTimeObject = {}
        print("*************")
        print("start_dt"+str(start_dt))
        print("end_dt"+str(end_dt))
        
        FetchDataForTimeObject['start_dt']=int(start_dt.timestamp() * 1000)
        FetchDataForTimeObject['end_dt']=int(end_dt.timestamp() * 1000) if end_dt else end_dt
        
        return FetchDataForTimeObject

    # Fetch full day arbitrage for 1 etf
    def FetchFullDayPerMinArbitrage(self, etfname):
        markettimeStatus=self.getMarketConditionsForFullDayData()
        if markettimeStatus['end_dt']:
            print("FetchFullDayPerMinArbitrage start "+ str(markettimeStatus['start_dt']))
            print("FetchFullDayPerMinArbitrage end "+ str(markettimeStatus['end_dt']))
            full_day_data_cursor = arbitrage_per_min.find(
            {"Timestamp": {"$gte": markettimeStatus['start_dt'],"$lte": markettimeStatus['end_dt']}, "ArbitrageData.symbol": etfname},
            {"_id": 0, "Timestamp": 1, "ArbitrageData.$": 1})
        else:
            print("FetchFullDayPerMinArbitrage start "+ str(markettimeStatus['start_dt']))
            full_day_data_cursor = arbitrage_per_min.find(
            {"Timestamp": {"$gte": markettimeStatus['start_dt']}, "ArbitrageData.symbol": etfname},
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
        markettimeStatus=self.getMarketConditionsForFullDayData()        
        if markettimeStatus['end_dt']:
            print("FetchFullDayPerMin Prices start "+ str(markettimeStatus['start_dt']))
            print("FetchFullDayPerMin Prices end "+ str(markettimeStatus['end_dt']))
            full_day_prices_etf_cursor = trade_per_min_WS.find({"e": {"$gte": markettimeStatus['start_dt'],'$lte':markettimeStatus['end_dt']}, "sym": etfname},
                                        {"_id": 0, "sym": 1, "vw": 1,"o":1,"c":1,"h":1,"l":1,"v":1,"e": 1})
        else:
            print("FetchFullDayPerMin Prices start "+ str(markettimeStatus['start_dt']))
            full_day_prices_etf_cursor = trade_per_min_WS.find({"e": {"$gte": markettimeStatus['start_dt']}, "sym": etfname},
                                        {"_id": 0, "sym": 1, "vw": 1,"o":1,"c":1,"h":1,"l":1,"v":1,"e": 1})
        temp = []
        [temp.append(item) for item in full_day_prices_etf_cursor]
        livePrices = pd.DataFrame.from_records(temp)
        livePrices.rename(columns={'sym': 'Symbol', 'vw': 'VWPrice','o':'open','c':'close','h':'high','l':'low','v':'TickVolume', 'e': 'date'}, inplace=True)
        livePrices.drop(columns=['Symbol'], inplace=True)
        print(livePrices)
        return livePrices

    #################################
    # Live Arbitrage & Price for 1 or all ETF
    #################################

    def getMarketConditionTime(self):
        now =  datetime.datetime.utcnow()
        currentTime = now.time()
        todaysDate = now.date()
        ifaholiday = HolidayCheck(todaysDate)
        dt=None
        # Current Market 930 to 4
        if (currentTime >= self.UTCStartTime) and (currentTime < self.UTCEndTime) and (not ifaholiday):
            dt = now.replace(second=0, microsecond=0)
            # dt = dt - datetime.timedelta(hours=self.daylightSavingAdjutment)
            dt = datetime.datetime.now().replace(second=0, microsecond=0)

        # After Market
        elif (currentTime >= self.UTCEndTime) and (currentTime < self.DAYendTime) and (not ifaholiday):
            dt = now.replace(hour=self.EndHour,minute=0,second=0, microsecond=0)
            # dt = dt - datetime.timedelta(hours=self.daylightSavingAdjutment)
            dt = dt.replace(tzinfo = tz.gettz('UTC'))
            dt = dt.astimezone(tz.tzlocal())

        # Next day of market before 9:30 am or holiday
        elif (currentTime > self.DAYendTimeZeroZeo) and (currentTime < datetime.time(self.StartHour,30)) or ifaholiday:
            dt=LastWorkingDay(todaysDate).replace(hour=self.EndHour,minute=0,second=0, microsecond=0)
            dt = dt.replace(tzinfo=tz.gettz('UTC'))
            dt = dt.astimezone(tz.tzlocal())
        # Fix for adjustment datetime to unix timestamp
        print("Live Single Arbitrage: "+str(dt))
        #dt = dt - datetime.timedelta(hours=self.daylightSavingAdjutment)
        return int(dt.timestamp() * 1000)

    #  Live arbitrage for 1 etf or all etf
    def LiveFetchPerMinArbitrage(self, etfname=None):
        dt_ts=self.getMarketConditionTime()
        print("LiveFetchPerMinArbitrage "+str(dt_ts))
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
        dt_ts=self.getMarketConditionTime()
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

    
    
