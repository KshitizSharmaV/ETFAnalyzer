class PolgonDataCreateURLS(object):
    apiKey = 'gd4V5GVT6ipvITQYEooyqUeLzaQ2gwmp'

    def PolygonLastQuotes(self, symbol):
        requesturl = 'https://api.polygon.io/v1/last_quote/stocks/' + symbol + '?apiKey='+self.apiKey
        return requesturl

    def PolygonLastTrades(self, symbol):
        # Make use of Tickers
        requesturl = 'https://api.polygon.io/v1/last/stocks/' + symbol + '?apiKey='+self.apiKey
        return requesturl

    def PolygonHistoricQuotes(self, symbol=None, date=None, startTS=None, endTS=None, limitresult=str(10),
                              aggregateBy=None):
        if startTS:
            # For Getting Paginated Request
            requesturl = 'https://api.polygon.io/v2/ticks/stocks/nbbo/' + symbol + '/' + date + '?timestamp=' + startTS + '&timestampLimit=' + endTS + '&limit=' + limitresult + '&apiKey='+self.apiKey
            print("Paginated Request For = " + symbol)
        else:
            requesturl = 'https://api.polygon.io/v2/ticks/stocks/nbbo/' + symbol + '/' + date + '?timestampLimit=' + endTS + '&limit=' + limitresult + '&apiKey='+self.apiKey
            print("First Request For = " + symbol)
        return requesturl

    def PolygonHistoricTrades(self, symbol=None, date=None, startTS=None, endTS=None, limitresult=str(10)):
        if startTS:
            # For Getting Paginated Request
            requesturl = 'https://api.polygon.io/v2/ticks/stocks/trades/' + symbol + '/' + date + '?timestamp=' + startTS + '&timestampLimit=' + endTS + '&limit=' + limitresult + '&apiKey='+self.apiKey
            print("Paginated Request For = " + symbol)
        else:
            requesturl = 'https://api.polygon.io/v2/ticks/stocks/trades/' + symbol + '/' + date + '?timestampLimit=' + endTS + '&limit=' + limitresult + '&apiKey='+self.apiKey
            print("First Request For = " + symbol)
        return requesturl

    def PolygonDailyOpenClose(self, date=None, symbol=None):
        requesturl = 'https://api.polygon.io/v1/open-close/' + symbol + '/' + date + '?apiKey='+self.apiKey
        return requesturl

    def PolygonAggregdateData(self, symbol=None, aggregateBy=None, startDate=None, endDate=None):
        # Make use of Tickers, Date and Limit
        requesturl = 'https://api.polygon.io/v2/aggs/ticker/' + symbol + '/range/1/' + aggregateBy + '/' + startDate + '/' + endDate + '?apiKey='+self.apiKey
        return requesturl
