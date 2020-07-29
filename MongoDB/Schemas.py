from pymongo import ASCENDING, DESCENDING
import getpass, socket
from MongoDB.MongoDBConnections import MongoDBConnectors

system_username = getpass.getuser()
system_private_ip = socket.gethostbyname(socket.gethostname())
if system_username == 'ubuntu' and system_private_ip == '172.31.76.32':
    ''' Production to Production:'''
    connection = MongoDBConnectors().get_pymongo_readWrite_production_production()
else:
    ''' Dev Local to Production ReadOnly:
        Will need to comment out create_index() statements to use this connection '''
    connection = MongoDBConnectors().get_pymongo_readonly_devlocal_production()
    ''' Dev Local to Production ReadWrite:
        Will need to specify username password file in MongoDB/MongoDBConnections.py __init__() '''
    # connection = MongoDBConnectors().get_pymongo_readWrite_devlocal_production()
    ''' Dev Local to Dev Local '''
    # connection = MongoDBConnectors().get_pymongo_devlocal_devlocal()

db = connection.ETF_db

if system_username == 'ubuntu' and system_private_ip == '172.31.76.32':
    ''' Production to Production:'''
    motor_client = MongoDBConnectors().get_motorasync_readWrite_production_production()
else:
    ''' Dev Local to Production ReadOnly:
        Will need to comment out create_index() statements to use this connection '''
    motor_client = MongoDBConnectors().get_motorasync_readonly_devlocal_production()
    ''' Dev Local to Production ReadWrite:
        Will need to specify username password file in MongoDB/MongoDBConnections.py __init__() '''
    # motor_client = MongoDBConnectors().get_motorasync_readWrite_devlocal_production()
    ''' Dev Local to Dev Local '''
    # motor_client = MongoDBConnectors().get_motorasync_devlocal_devlocal()

motor_db = motor_client.ETF_db

# Quotes Pipeline
quotesCollection = db.QuotesData
if system_username == 'ubuntu' and system_private_ip == '172.31.76.32':
    quotesCollection.create_index([("dateForData", DESCENDING), ("symbol", ASCENDING)])
quotespipeline = [
    {'$match': ''},
    {'$unwind': '$data'},
    {'$group': {
        '_id': '$_id',
        'data': {'$push': {
            'Symbol': '$data.Symbol',
            'Time': '$data.t',
            'Bid Price': '$data.p',
            'Bid Size': '$data.s',
            'Ask Price': '$data.P',
            'Ask Size': '$data.S',
        }}
    }}
]

# Trades Pipeline
tradeCollection = db.TradesData
if system_username == 'ubuntu' and system_private_ip == '172.31.76.32':
    tradeCollection.create_index([("dateForData", DESCENDING), ("symbol", ASCENDING)])
tradespipeline = [
    {'$match': ''},
    {'$unwind': '$data'},
    {'$group': {
        '_id': '$_id',
        'data': {'$push': {
            'Symbol': '$data.Symbol',
            'Time': '$data.t',
            'High Price': '$data.h',
            'Low Price': '$data.l',
            'Trade Size': '$data.v',
            'Number of Trades': '$data.n',
        }}
    }}
]

# Daily Open Close Collection
dailyopencloseCollection = db.DailyOpenCloseCollection
if system_username == 'ubuntu' and system_private_ip == '172.31.76.32':
    dailyopencloseCollection.create_index([("dateForData", DESCENDING), ("Symbol", ASCENDING)], unique=True)

# Arbitrage
arbitragecollection = db.ArbitrageCollectionNew
if system_username == 'ubuntu' and system_private_ip == '172.31.76.32':
    arbitragecollection.create_index([("dateOfAnalysis", DESCENDING), ("ETFName", ASCENDING)])

# Arbitrage Per Minute
arbitrage_per_min = db.ArbitragePerMin
if system_username == 'ubuntu' and system_private_ip == '172.31.76.32':
    arbitrage_per_min.create_index([('Timestamp', DESCENDING)])

# Trade Aggregate Minute for all Tickers.
# Cursor for pulling data (PyMongo Cursor)
trade_per_min_WS = db.TradePerMinWS
# Cursor for insert action into TradePerMinWS (AsyncIOMotorCursor)
trade_per_min_WS_motor = motor_db.TradePerMinWS

quotesWS_collection = db.QuotesLiveData

etfholdings_collection = db.ETFHoldings

connection.close()
