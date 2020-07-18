import datetime
from MongoDB.Schemas import trade_per_min_WS_motor, trade_per_min_WS, quotesWS_collection, arbitrage_per_min
import pandas as pd
from time import time
from dateutil import tz
from CommonServices.Holidays import HolidayCheck, LastWorkingDay, isTimeBetween


class PerMinDataOperations():

    def __init__(self):
        ''' Day End Times in UTC'''
        self.DAYendTime = datetime.time(3, 59) if datetime.date(2020, 3,
                                                                8) < datetime.datetime.now().date() < datetime.date(
            2020, 11, 1) else datetime.time(4, 59)
        # self.DAYendTime = datetime.time(23,59)
        self.DAYendTimeZeroZeo = datetime.time(3, 59,59) if datetime.date(2020, 3,
                                                                       8) < datetime.datetime.now().date() < datetime.date(
            2020, 11, 1) else datetime.time(4, 59,59)
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
        now = datetime.datetime.utcnow()
        currentTime = now.time()
        todaysDate = now.date()
        ifaholiday = HolidayCheck(todaysDate)
        end_dt = None
        if currentTime >= self.UTCStartTime and (not ifaholiday):
            start_dt = now
            start_dt = start_dt.replace(hour=self.StartHour, minute=30, second=0, microsecond=0)
            # start_dt=start_dt - datetime.timedelta(hours=self.daylightSavingAdjutment)
            start_dt = start_dt.replace(tzinfo=tz.gettz('UTC'))
            start_dt = start_dt.astimezone(tz.tzlocal())
        else:
            lastworkinDay = LastWorkingDay(todaysDate - datetime.timedelta(days=1))
            start_dt = lastworkinDay

            start_dt = start_dt.replace(hour=self.StartHour, minute=30, second=0, microsecond=0)
            start_dt = start_dt.replace(tzinfo=tz.gettz('UTC'))
            start_dt = start_dt.astimezone(tz.tzlocal())

            end_dt = lastworkinDay.replace(hour=self.EndHour, minute=00, second=0, microsecond=0)
            end_dt = end_dt.replace(tzinfo=tz.gettz('UTC'))
            end_dt = end_dt.astimezone(tz.tzlocal())
            # end_dt=end_dt - datetime.timedelta(hours=self.daylightSavingAdjutment)

        # start_dt = start_dt.replace(hour=self.StartHour,minute=30,second=0, microsecond=0)
        # Fix for breaking code
        # start_dt=start_dt - datetime.timedelta(hours=self.daylightSavingAdjutment)

        FetchDataForTimeObject = {}
        # print("*************")
        # print("start_dt" + str(start_dt))
        # print("end_dt" + str(end_dt))

        FetchDataForTimeObject['start_dt'] = int(start_dt.timestamp() * 1000)
        FetchDataForTimeObject['end_dt'] = int(end_dt.timestamp() * 1000) if end_dt else end_dt

        return FetchDataForTimeObject

    # Fetch full day arbitrage for 1 etf
    def FetchFullDayPerMinArbitrage(self, etfname):
        markettimeStatus = self.getMarketConditionsForFullDayData()
        if markettimeStatus['end_dt']:
            print("FetchFullDayPerMinArbitrage start " + str(markettimeStatus['start_dt']))
            print("FetchFullDayPerMinArbitrage end " + str(markettimeStatus['end_dt']))
            full_day_data_cursor = arbitrage_per_min.find(
                {"Timestamp": {"$gte": markettimeStatus['start_dt'], "$lte": markettimeStatus['end_dt']},
                 "ArbitrageData.symbol": etfname},
                {"_id": 0, "Timestamp": 1, "ArbitrageData.$": 1})
        else:
            print("FetchFullDayPerMinArbitrage start " + str(markettimeStatus['start_dt']))
            full_day_data_cursor = arbitrage_per_min.find(
                {"Timestamp": {"$gte": markettimeStatus['start_dt']}, "ArbitrageData.symbol": etfname},
                {"_id": 0, "Timestamp": 1, "ArbitrageData.$": 1})

        data = []
        getTimeStamps = []
        for item in full_day_data_cursor:
            getTimeStamps.append(item['Timestamp'])
            data.extend(item['ArbitrageData'])
        full_day_data_df = pd.DataFrame.from_records(data)
        full_day_data_df['Timestamp'] = getTimeStamps
        return full_day_data_df

    # Full full  day prices for 1 etf
    def FetchFullDayPricesForETF(self, etfname):
        markettimeStatus = self.getMarketConditionsForFullDayData()
        if markettimeStatus['end_dt']:
            print("FetchFullDayPerMin Prices start " + str(markettimeStatus['start_dt']))
            print("FetchFullDayPerMin Prices end " + str(markettimeStatus['end_dt']))
            full_day_prices_etf_cursor = trade_per_min_WS.find(
                {"e": {"$gte": markettimeStatus['start_dt'], '$lte': markettimeStatus['end_dt']}, "sym": etfname},
                {"_id": 0, "sym": 1, "vw": 1, "o": 1, "c": 1, "h": 1, "l": 1, "v": 1, "e": 1})
        else:
            print("FetchFullDayPerMin Prices start " + str(markettimeStatus['start_dt']))
            full_day_prices_etf_cursor = trade_per_min_WS.find(
                {"e": {"$gte": markettimeStatus['start_dt']}, "sym": etfname},
                {"_id": 0, "sym": 1, "vw": 1, "o": 1, "c": 1, "h": 1, "l": 1, "v": 1, "e": 1})
        temp = []
        [temp.append(item) for item in full_day_prices_etf_cursor]
        livePrices = pd.DataFrame.from_records(temp)
        livePrices.rename(columns={'sym': 'symbol', 'vw': 'VWPrice', 'o': 'open', 'c': 'close', 'h': 'high', 'l': 'low',
                                   'v': 'TickVolume', 'e': 'date'}, inplace=True)
        livePrices.drop(columns=['symbol'], inplace=True)
        return livePrices

    #################################
    # Live Arbitrage & Price for 1 or all ETF
    #################################

    def getMarketConditionTime(self):
        ''''''
        '''Current UTC datetime'''
        now = datetime.datetime.utcnow()
        '''Current time only'''
        currentTime = now.time()
        '''Current date only'''
        todaysDate = now.date()
        ifaholiday = HolidayCheck(todaysDate)
        dt=None

        # Testing - KTZ
        # print(now)
        # print(currentTime)
        # print(self.UTCEndTime)
        # print(self.DAYendTime)
        # print((not ifaholiday))
        # print(self.DAYendTimeZeroZeo)
        # print(datetime.time(self.StartHour, 30))

        ##########################################################################################

        '''Piyush's Logic'''
        '''Market operating time on a Non-Holiday UTC 13:30:00 (self.UTCStartTime) to 20:00:00 (self.UTCEndTime)'''
        if (currentTime >= self.UTCStartTime) and (currentTime < self.UTCEndTime) and (not ifaholiday):
            print("LINE 168")
            dt = datetime.datetime.now().replace(second=0, microsecond=0)

        '''Before Market opens same day UTC 4:00:00 (self.DAYendTimeZeroZeo) to 13:30:00 (self.UTCStartTime) OR if it's a holiday'''
        if currentTime > self.DAYendTimeZeroZeo and currentTime < self.UTCStartTime or ifaholiday:
            print("LINE 172")
            dt = LastWorkingDay(todaysDate - datetime.timedelta(days=1)).replace(hour=self.EndHour, minute=0, second=0,
                                                                                 microsecond=0)
            dt = dt.replace(tzinfo=tz.gettz('UTC'))
            dt = dt.astimezone(tz.tzlocal())

        '''Here 2 conditions will be used.'''
        '''After market closes same day UTC 20:00:00 (self.UTCEndTime) to 23:59:59 (UTC Date Change Time). On a Non-Holiday.'''
        if currentTime < datetime.time(23, 59, 59) and currentTime > self.UTCEndTime and (not ifaholiday):
            print("LINE 182")
            dt = now.replace(hour=self.EndHour, minute=0, second=0, microsecond=0)
            dt = dt.replace(tzinfo=tz.gettz('UTC'))
            dt = dt.astimezone(tz.tzlocal())
        '''After market closes 00:00:00 (UTC New Date Time) to 3:59:59 (self.DAYendTimeZeroZeo). Doesn't matter if it's a holiday.'''
        if currentTime > datetime.time(0, 0) and currentTime < self.DAYendTimeZeroZeo:
            print("LINE 188")
            dt = LastWorkingDay(todaysDate - datetime.timedelta(days=1)).replace(hour=self.EndHour, minute=0, second=0,
                                                                                 microsecond=0)
            dt = dt.replace(tzinfo=tz.gettz('UTC'))
            dt = dt.astimezone(tz.tzlocal())

        ##########################################################################################
        '''Kshitiz's Logic'''
        '''
        # Current Market 930 to 4
        if (currentTime >= self.UTCStartTime) and (currentTime < self.UTCEndTime) and (not ifaholiday):
            dt = now.replace(second=0, microsecond=0)
            # dt = dt - datetime.timedelta(hours=self.daylightSavingAdjutment)
            dt = datetime.datetime.now().replace(second=0, microsecond=0)

        # After Market
        #elif (currentTime >= self.UTCEndTime) and (currentTime < self.DAYendTime) and (not ifaholiday): - KTZ removed this
        elif (currentTime >= self.UTCEndTime) and (not ifaholiday):
            dt = now.replace(hour=self.EndHour,minute=0,second=0, microsecond=0)
            # dt = dt - datetime.timedelta(hours=self.daylightSavingAdjutment)
            dt = dt.replace(tzinfo = tz.gettz('UTC'))
            dt = dt.astimezone(tz.tzlocal())

        # Next day of market before 9:30 am or holiday
        elif (currentTime > self.DAYendTimeZeroZeo) and (currentTime < self.UTCStartTime) or ifaholiday:
            dt=LastWorkingDay(todaysDate-datetime.timedelta(days=1)).replace(hour=self.EndHour,minute=0,second=0, microsecond=0)
            dt = dt.replace(tzinfo=tz.gettz('UTC'))
            dt = dt.astimezone(tz.tzlocal())
        '''
        ##########################################################################################
        # Fix for adjustment datetime to unix timestamp
        # print("getMarketConditionTime returning : " + str(dt))
        # dt = dt - datetime.timedelta(hours=self.daylightSavingAdjutment)
        return int(dt.timestamp() * 1000)

    #  Live arbitrage for 1 etf or all etf
    def LiveFetchPerMinArbitrage(self, etfname=None):
        dt_ts = self.getMarketConditionTime()
        print("LiveFetchPerMinArbitrage " + str(dt_ts))
        data = []
        if etfname:
            live_per_min_cursor = arbitrage_per_min.find(
                {"Timestamp": dt_ts, "ArbitrageData.symbol": etfname},
                {"_id": 0, "Timestamp": 1, "ArbitrageData.$": 1})

            data = []
            getTimeStamps = []
            for item in live_per_min_cursor:
                getTimeStamps.append(item['Timestamp'])
                data.extend(item['ArbitrageData'])
            liveArbitrageData_onemin = pd.DataFrame.from_records(data)
            liveArbitrageData_onemin['Timestamp'] = getTimeStamps

        else:
            # Data For Multiple Ticker for live minute
            live_per_min_cursor = arbitrage_per_min.find(
                {"Timestamp": dt_ts},
                {"_id": 0, "Timestamp": 1, "ArbitrageData": 1})
            result_list = list(live_per_min_cursor)
            data = []
            ts = [item['Timestamp'] for item in result_list]
            [data.extend(item['ArbitrageData']) for item in result_list]
            liveArbitrageData_onemin = pd.DataFrame.from_records(data)

        return liveArbitrageData_onemin, ts

    # LIVE 1 Min prices for 1 or all etf
    def LiveFetchETFPrice(self, etfname=None):
        dt_ts = self.getMarketConditionTime()
        print("LiveFetchETFPrice " + str(dt_ts))
        if etfname:
            etf_live_prices_cursor = trade_per_min_WS.find({"e": {'$lte':dt_ts}, "sym": etfname},
                                                           {"_id": 0, "sym": 1, "vw": 1, "o": 1, "c": 1, "h": 1, "l": 1,
                                                            "v": 1, "e": 1}).sort([('e',-1)]).limit(1)
        else:
            etf_live_prices_cursor = trade_per_min_WS.find({"e": dt_ts},
                                                           {"_id": 0, "sym": 1, "vw": 1, "o": 1, "c": 1, "h": 1, "l": 1,
                                                            "v": 1, "e": 1})

        temp = []
        [temp.append(item) for item in etf_live_prices_cursor]
        livePrices = pd.DataFrame.from_records(temp)
        livePrices.rename(columns={'sym': 'symbol', 'vw': 'VWPrice', 'o': 'open', 'c': 'close', 'h': 'high', 'l': 'low',
                                   'v': 'TickVolume', 'e': 'date'}, inplace=True)
        if etfname:
            livePrices.drop(columns=['symbol'], inplace=True)
        return livePrices
