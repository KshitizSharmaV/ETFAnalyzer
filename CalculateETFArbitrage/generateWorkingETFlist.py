import csv
import pandas as pd
import pymongo
import datetime
from MongoDB.Schemas import arbitragecollection

agr = [
    {
        '$group': {
            '_id': '$dateOfAnalysis',
            'count': {
                '$sum': 1
            }
        }
    }, {
        '$sort': {
            'count': 1
        }
    }
]
resdate = arbitragecollection.aggregate(agr)
result = [res for res in resdate]
datelist = [res['_id'] for res in result]
print(datelist)
resultdf = pd.DataFrame.from_records(result)
print(resultdf)

WorkingETFset = set()
for date in datelist:
    res = arbitragecollection.find({'dateOfAnalysis': date},{'_id':0,'ETFName':1})
    df = pd.DataFrame.from_records([etf for etf in res])
    if len(WorkingETFset)>0:
        WorkingETFset.intersection(set(df['ETFName'].to_list()))
    else:
        WorkingETFset.update(df['ETFName'].to_list())

with open('newWorkingETFs.csv', 'w') as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow(list(WorkingETFset))

print(WorkingETFset)
print(len(WorkingETFset))