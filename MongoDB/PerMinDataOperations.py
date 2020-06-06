import datetime
from MongoDB.Schemas import trade_per_min_WS_motor, trade_per_min_WS, quotesWS_collection, arbitrage_per_min


class PerMinDataOperations():

    # Use AsyncIOMotorCursor for inserting into TradePerMinWS Collection
    async def do_insert(self, data):
        result = await trade_per_min_WS_motor.insert_many(data)
        print('inserted %d docs' % (len(result.inserted_ids),))

    # Insert into QuotesLiveData Collection
    def insertQuotesLive(self, quotesData):
        quotesWS_collection.insert_many(quotesData, ordered=False)

    # Use PyMongo Cursor for fetching from TradePerMinWS Collection
    def FetchAllTradeDataPerMin(self, DateTimeOfTrade):
        dt = datetime.datetime.strptime(DateTimeOfTrade, '%Y-%m-%d %H:%M')
        dt_ts = int(dt.timestamp() * 1000)
        all_tickers_data = trade_per_min_WS.find({'e': dt_ts}, {'_id': 0, 'sym': 1, 'vw': 1})
        return all_tickers_data

    # Fetch from QuotesLiveData Collection
    def FetchQuotesLiveDataForSpread(self, startts, endts):
        quotes_data_for_etf = quotesWS_collection.find({'t': {'$gt': startts, '$lte': endts}},
                                                       {'sym': 1, 'ap': 1, 'bp': 1})
        return quotes_data_for_etf


    #################################
    # Hostorical Arbitrage & Price for a day
    #################################

    # Fetch full day arbitrage for 1 etf
    def FetchFullDayPerMinArbitrage(self, etfname):
        day_start_dt = datetime.datetime.strptime(' '.join([str(datetime.datetime.now().date()), '09:00']),
                                                  '%Y-%m-%d %H:%M')
        # Testing Remove in prod
        day_start_dt = datetime.datetime.strptime(' '.join([str(datetime.datetime.now().date()-datetime.timedelta(1)), '09:00']),
                                                  '%Y-%m-%d %H:%M')
        day_end_dt = datetime.datetime.strptime(' '.join([str(datetime.datetime.now().date()), '08:00']),'%Y-%m-%d %H:%M')
        day_end_dt = int(day_end_dt.timestamp() * 1000)
        # Testing Remove in prod

        day_start_ts = int(day_start_dt.timestamp() * 1000)
        full_day_data_cursor = arbitrage_per_min.find(
            {"Timestamp": {"$gte": day_start_ts,"$lt":day_end_dt}, "ArbitrageData.Symbol": etfname},
            {"_id": 0, "Timestamp": 1, "ArbitrageData.$": 1})
        return full_day_data_cursor


    # Full full  day prices for 1 etf
    def FetchFullDayPricesForETF(self, etfname):
        day_start_dt = datetime.datetime.strptime(' '.join([str(datetime.datetime.now().date()), '09:00']),'%Y-%m-%d %H:%M')
        # Testing Remove in prod
        day_start_dt = datetime.datetime.strptime(' '.join([str(datetime.datetime.now().date()-datetime.timedelta(1)), '09:00']),'%Y-%m-%d %H:%M')
        day_end_dt = datetime.datetime.strptime(' '.join([str(datetime.datetime.now().date()), '08:00']),'%Y-%m-%d %H:%M')
        day_end_dt = int(day_end_dt.timestamp() * 1000)
        # Testing Remove in prod
        day_start_ts = int(day_start_dt.timestamp() * 1000)
        full_day_prices_etf_cursor = trade_per_min_WS.find({"e": {"$gte": day_start_ts,"$lt":day_end_dt}, "sym": etfname},
                                                           {"_id": 0, "sym": 1, "vw": 1,"o":1,"c":1,"h":1,"l":1,"v":1,"e": 1})
        return full_day_prices_etf_cursor

    #################################
    # Live Arbitrage & Price for 1 or all ETF
    #################################

    #  Live arbitrage for 1 etf or all etf
    def LiveFetchPerMinArbitrage(self, etfname=None):
        dt = datetime.datetime.now().replace(second=0, microsecond=0)
        dt = datetime.datetime(2020, 6, 4, 15, 59)
        print(dt)
        dt_ts = int(dt.timestamp() * 1000)
        print(dt_ts)
        if etfname:
            live_per_min_cursor = arbitrage_per_min.find(
                {"Timestamp": dt_ts, "ArbitrageData.Symbol": etfname},
                {"_id": 0, "Timestamp": 1, "ArbitrageData.$": 1})
        else:
            live_per_min_cursor = arbitrage_per_min.find(
                {"Timestamp": dt_ts},
                {"_id": 0, "Timestamp": 1, "ArbitrageData": 1})

        return live_per_min_cursor

    # LIVE 1 Min prices for 1 or all etf
    def LiveFetchETFPrice(self, etfname=None):
        dt = datetime.datetime.now().replace(second=0, microsecond=0)
        dt = datetime.datetime(2020, 6, 4, 15, 59)
        dt_ts = int(dt.timestamp() * 1000)
        if etfname:
            etf_live_prices_cursor = trade_per_min_WS.find({"e": dt_ts,"sym": etfname}, {"_id": 0, "sym": 1, "vw": 1, "e": 1})
        else:
            etf_live_prices_cursor = trade_per_min_WS.find({"e": dt_ts}, {"_id": 0, "sym": 1, "vw": 1,"o":1,"c":1,"h":1,"l":1,"v":1, "e": 1})
        return etf_live_prices_cursor

    
    
