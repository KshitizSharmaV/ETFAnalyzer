import sys, traceback

# For Piyush System
sys.path.extend(['/home/piyush/Desktop/etf0406', '/home/piyush/Desktop/etf0406/ETFAnalyzer',
                 '/home/piyush/Desktop/etf0406/ETFAnalyzer/ETFsList_Scripts',
                 '/home/piyush/Desktop/etf0406/ETFAnalyzer/HoldingsDataScripts',
                 '/home/piyush/Desktop/etf0406/ETFAnalyzer/CommonServices',
                 '/home/piyush/Desktop/etf0406/ETFAnalyzer/CalculateETFArbitrage'])
# For Production env
sys.path.extend(['/home/ubuntu/ETFAnalyzer', '/home/ubuntu/ETFAnalyzer/ETFsList_Scripts',
                 '/home/ubuntu/ETFAnalyzer/HoldingsDataScripts', '/home/ubuntu/ETFAnalyzer/CommonServices',
                 '/home/ubuntu/ETFAnalyzer/CalculateETFArbitrage'])
sys.path.append("..")  # Remove in production - KTZ
from mongoengine import *
from pymongo import *
import motor.motor_asyncio
import getpass


class MongoDBConnectors():
    def __init__(self):
        if getpass.getuser() == 'ubuntu':
            self.path = '/home/ubuntu/ETFAnalyzer/'
        elif getpass.getuser() == 'piyush':
            self.path = '/home/piyush/Desktop/etf0406/ETFAnalyzer/'
        else:

            self.path = ''

    ''' PyMongo Connections '''

    def get_pymongo_readonly_devlocal_production(self):
        connection = MongoClient('18.213.229.80', 27017, username='usertesterReadOnly', password='onlyreadpass')
        return connection

    def get_pymongo_readWrite_devlocal_production(self):
        try:
            with open(self.path + "MongoDBAccInfo.txt") as f:
                credentials = [x.strip().split(':', 1) for x in f]
            username = credentials[0][0]
            password = credentials[0][1]
        except:
            username = None
            password = None
            pass
        connection = MongoClient('18.213.229.80', 27017, username=username, password=password)
        return connection

    def get_pymongo_devlocal_devlocal(self):
        connection = MongoClient('localhost', 27017)
        return connection

    def get_pymongo_readonly_production_production(self):
        connection = MongoClient('localhost', 27017,username='usertesterReadOnly',
                                 password='onlyreadpass')
        return connection

    def get_pymongo_readWrite_production_production(self):
        try:
            with open(self.path + "MongoDBAccInfo.txt") as f:
                credentials = [x.strip().split(':', 1) for x in f]
            username = credentials[0][0]
            password = credentials[0][1]
        except:
            username = None
            password = None
            pass
        connection = MongoClient('localhost', 27017, username=username, password=password)
        return connection

    ''' MongoEngine Connections '''

    def get_mongoengine_readonly_devlocal_production(self):
        return connect('ETF_db', alias='ETF_db', host='18.213.229.80', port=27017, username='usertesterReadOnly',
                       password='onlyreadpass', authentication_source='admin')

    def get_mongoengine_readWrite_devlocal_production(self):
        try:
            with open(self.path + "MongoDBAccInfo.txt") as f:
                credentials = [x.strip().split(':', 1) for x in f]
            username = credentials[0][0]
            password = credentials[0][1]
        except:
            username = None
            password = None
            pass
        return connect('ETF_db', alias='ETF_db', host='18.213.229.80', port=27017, username=username, password=password,
                       authentication_source='admin')

    def get_mongoengine_devlocal_devlocal(self):
        return connect('ETF_db', alias='ETF_db')

    def get_mongoengine_readonly_production_production(self):
        return connect('ETF_db', alias='ETF_db', username='usertesterReadOnly',
                       password='onlyreadpass', authentication_source='admin')

    def get_mongoengine_readWrite_production_production(self):
        try:
            with open(self.path + "MongoDBAccInfo.txt") as f:
                credentials = [x.strip().split(':', 1) for x in f]
            username = credentials[0][0]
            password = credentials[0][1]
        except:
            username = None
            password = None
            pass
        return connect('ETF_db', alias='ETF_db', username=username, password=password,
                       authentication_source='admin')
    ''' Motor AsyncIO Connections '''
    def get_motorasync_readonly_devlocal_production(self):
        connection = motor.motor_asyncio.AsyncIOMotorClient('18.213.229.80', 27017, username='usertesterReadOnly', password='onlyreadpass')
        return connection

    def get_motorasync_readWrite_devlocal_production(self):
        try:
            with open(self.path + "MongoDBAccInfo.txt") as f:
                credentials = [x.strip().split(':', 1) for x in f]
            username = credentials[0][0]
            password = credentials[0][1]
        except:
            username = None
            password = None
            pass
        connection = motor.motor_asyncio.AsyncIOMotorClient('18.213.229.80', 27017, username=username, password=password)
        return connection

    def get_motorasync_devlocal_devlocal(self):
        connection = motor.motor_asyncio.AsyncIOMotorClient('localhost', 27017)
        return connection

    def get_motorasync_readonly_production_production(self):
        connection = motor.motor_asyncio.AsyncIOMotorClient('localhost', 27017, username='usertesterReadOnly',
                                 password='onlyreadpass')
        return connection

    def get_motorasync_readWrite_production_production(self):
        try:
            with open(self.path + "MongoDBAccInfo.txt") as f:
                credentials = [x.strip().split(':', 1) for x in f]
            username = credentials[0][0]
            password = credentials[0][1]
        except:
            username = None
            password = None
            pass
        connection = motor.motor_asyncio.AsyncIOMotorClient('localhost', 27017, username=username, password=password)
        return connection