import sys
import time

sys.path.append('..')
import pymongo
import pandas as pd
from MongoDB.MongoDBConnections import MongoDBConnectors

conn = MongoDBConnectors().get_pymongo_readWrite_production_production()
collread = conn.ETF_db.ArbitrageCollection
collwrite = conn.ETF_db.ArbitrageCollectionNew

cursor = collread.find()
data = []
for item in cursor:
    etf = item['ETFName']
    doa = item['dateOfAnalysis']
    dwar = item['dateWhenAnalysisRan']
    ad = item['data']
    df = pd.DataFrame.from_records(ad)
    cols = list(df.columns)
    df['ETFName'] = etf
    df['dateOfAnalysis'] = doa
    df['dateWhenAnalysisRan'] = dwar
    df = df[['ETFName','dateOfAnalysis','dateWhenAnalysisRan']+cols]
    print(df)
    data.append(df)

for df in data:
    collwrite.insert_many(df.to_dict(orient='records'))

# '''Speed Comparison'''
# start2 = time.time()
# newstruct = collwrite.find(
#     {"Timestamp": {"$gte": 1594903920000}, "symbol": 'XLK'},
#     {"_id": 0}
# )
# new_data = list(newstruct)
# print(len(new_data))
# end2 = time.time()
# print("New: {}".format(end2-start2))
#
# start = time.time()
# oldstruct = collread.find(
#                 {"Timestamp": {"$gte": 1594903920000}, "ArbitrageData.symbol": 'XLK'},
#                 {"_id": 0, "Timestamp": 1, "ArbitrageData.$": 1})
# old_data = list(oldstruct)
# print(len(old_data))
# end = time.time()
# print("OLD: {}".format(end-start))


