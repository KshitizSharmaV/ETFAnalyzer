import sys
import json
import datetime
import traceback

sys.path.append("..")
import pandas as pd
from CalculateETFArbitrage.LoadEtfHoldings import LoadHoldingsdata
from CalculateETFArbitrage.GetRelevantHoldings import RelevantHoldings
from CommonServices.MultiProcessingTasks import CPUBonundThreading
from MongoDB.Schemas import etfholdings_collection

class ListsCreator():

    def convertDataToDict(self,df,etfname):
        df.drop(columns=['TickerName'], inplace=True)
        df.rename(columns={'TickerSymbol':'symbol', 'TickerWeight':'weight'},inplace=True)
        res={}
        res['HoldingsList'] = df['symbol'].to_list()
        res['ETFHoldingsData'] = {etfname: [df.to_dict()]}
        return res

    def raiseError(self,errorType=None):
        # Raise Error & send email For failling to add data to etf-hold.json
        if errorType==2:
            #mail("Failed To Load Data For ETF-Hold.json on second try also + etfname")
            print("Failed To Load Data For ETF-Hold.json")
            print(df)
            print(etfname)
            print((datetime.datetime.now() - datetime.timedelta(days=1)))
            traceback.print_exc()
            return None
        else:
            print("Error Type 1: Failed From CalculateETFArbitrage,Going for 2nd Try through PyMongo")
            
    def ETFHoldJsonData(self,etfname):
        try:
            df=None
            # Fetch data using Yesterday's date
            df = LoadHoldingsdata().getHoldingsDatafromDB(etfname,(datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y-%m-%d"))
            return self.convertDataToDict(df,etfname)
        except Exception as e:
            self.raiseError(errorType=1)
            # Get the last date from the mongodb this time usine Schemas.py Pymongo, we are fetching last occurence
            etfdata=etfholdings_collection.find({'ETFTicker':etfname}).sort("FundHoldingsDate",-1).limit(1)
            for data in etfdata:
                df = pd.DataFrame(data['holdings'])
            return self.convertDataToDict(df,etfname) if df is not None else self.raiseError(errorType=2)
                
    def create_list_files(self):
        try:
            AllEtfNames = list(pd.read_csv("../CSVFiles/NonChineseETFs.csv").columns.values)
            
            ThreadingResults = CPUBonundThreading(self.ETFHoldJsonData, AllEtfNames)

            AllHoldingsList=[]
            etfdicts = []
            for res in ThreadingResults:
                etfdicts.append(res['ETFHoldingsData'])
                AllHoldingsList=AllHoldingsList+res['HoldingsList']

            #print(len(etfdicts))
            out_file = open("../CSVFiles/etf-hold.json", "w")
            json.dump(etfdicts, out_file, indent=6)
            out_file.close()

            AllTickerSet = set(AllHoldingsList+AllEtfNames)
            #print(len(AllTickerSet))
            RelevantHoldings().write_to_csv(etflist=list(AllTickerSet), filename="../CSVFiles/tickerlist.csv")
            return "Tick Lists Generated Successfully"
        except Exception as e:
            errorMessage = "####*** Error in generating Tick List in TickListsGenerator.py" + '\n'+traceback.print_exc()
            return errorMessage

if __name__=='__main__':
    ListsCreator().create_list_files()