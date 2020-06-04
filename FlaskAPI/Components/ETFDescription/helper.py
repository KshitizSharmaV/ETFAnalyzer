from mongoengine import connect
import datetime
from PolygonTickData.HistoricOHLCgetter import HistoricOHLC
from PolygonTickData.Helper import Helper

def fetchETFsWithSameIssuer(connection=None, Issuer=None):
    CollectionName = connection.ETF_db.ETFHoldings
    query = {'Issuer': Issuer}
    dataD = CollectionName.find(query, {'FundHoldingsDate': 1, 'ETFTicker': 1, 'TotalAssetsUnderMgmt': 1, 'ETFName': 1,
                                        '_id': 0}).sort('-FundHoldingsDate')
    # Search How to loop over mongo cursor and extract data - Do Something
    ETFWithSameIssuer = {}
    for item in dataD:
        if item['ETFTicker'] not in list(item.keys()):
            ETFWithSameIssuer[item['ETFTicker']] = {'ETFName': item['ETFName'],
                                               'TotalAssetsUnderMgmt': "${:,.3f} M".format(item['TotalAssetsUnderMgmt']/1000)}
    return ETFWithSameIssuer


def fetchETFsWithSameETFdbCategory(connection=None,ETFdbCategory=None):
    CollectionName = connection.ETF_db.ETFHoldings
    # Find ETFs with same ETFdbCategory
    data = CollectionName.find({'ETFdbCategory': ETFdbCategory},
                               {'FundHoldingsDate': 1, 'ETFTicker': 1, 'TotalAssetsUnderMgmt': 1, 'ETFName': 1,
                                '_id': 0}).sort('-FundHoldingsDate')
    ETFWithSameETFDBCategory = {}
    for item in data:
        if item['ETFTicker'] not in list(item.keys()):
            ETFWithSameETFDBCategory[item['ETFTicker']] = {'ETFName': item['ETFName'],
                                               'TotalAssetsUnderMgmt': "${:,.3f} M".format(item['TotalAssetsUnderMgmt']/1000)}
    return ETFWithSameETFDBCategory


def fetchETFsWithSimilarTotAsstUndMgmt(connection=None,totalassetUnderManagement=None):
    CollectionName = connection.ETF_db.ETFHoldings
    # TotalAssetUnderMgmt for given ETF + 10%
    taumpos = totalassetUnderManagement * 1.50
    # TotalAssetUnderMgmt for given ETF - 10%
    taumneg = totalassetUnderManagement * 0.50
    # Find ETFs with TotalAssetUnderMgmt +/- 10% of given ETF's
    similar_taum_etfs = CollectionName.find(
        {'TotalAssetsUnderMgmt': {'$lte': taumpos, '$gte': taumneg}},
        {'FundHoldingsDate': 1, 'ETFTicker': 1, 'TotalAssetsUnderMgmt': 1, 'ETFName': 1,
         '_id': 0}).sort('-FundHoldingsDate')
    # List of Dicts
    ETFWithSameAssetUnderManagement={}
    for item in similar_taum_etfs:
        ETFWithSameAssetUnderManagement[item['ETFTicker']]={'ETFName': item['ETFName'], 
                                                            'TotalAssetsUnderMgmt': "${:,.3f} M".format(item['TotalAssetsUnderMgmt']/1000)}
    return ETFWithSameAssetUnderManagement

def fetchOHLCHistoricalData(etfname=None,StartDate=None):
    ob = HistoricOHLC()
    data=ob.getopenlowhistoric(etfname=etfname, startdate=StartDate)
    del data['n']
    data.rename(columns={'v': 'volume',
                         'o': 'open',
                         'c': 'close',
                         'h': 'high',
                         'l': 'low',
                         't': 'date'}, inplace=True)
    # Helper Class from PolygonTickData.Helper for converting uni timestamp to human timestamp
    helperObjTimeConversion = Helper()
    data['date']=data['date'].apply(lambda x: helperObjTimeConversion.getHumanTime(ts=x, divideby=1000))
    print(data)
    return data
