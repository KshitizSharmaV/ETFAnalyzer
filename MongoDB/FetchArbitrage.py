from _datetime import datetime
from MongoDB.Schemas import arbitragecollection

class FetchArbitrage():
    def fetch_arbitrage_data(self, dateofanalysis):
        data = arbitragecollection.find({'dateOfAnalysis':datetime.strptime(dateofanalysis, '%Y-%m-%d')})
        return data