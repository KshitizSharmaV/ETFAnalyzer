import traceback
from flask import Response
from CalculateETFArbitrage.Helpers.LoadEtfHoldings import LoadHoldingsdata
from MongoDB.Schemas import MongoDBConnectors
import numpy as np
import pandas as pd

def ETFandHoldingsData(ETFName, date):
    try:
        MongoDBConnectors().get_mongoengine_readonly_devlocal_production()
        # Load all the data holdings data together
        etfdata = LoadHoldingsdata().getAllETFData(ETFName, date)
        if type(etfdata) == Response:
            return etfdata
        ETFDataObject = etfdata.to_mongo().to_dict()
        print(ETFDataObject)
        HoldingsDatObject = pd.DataFrame(ETFDataObject['holdings']).set_index('TickerSymbol').T.to_dict()


        ETFDataObject['TotalAssetsUnderMgmt'] = "${:,.3f} M".format(ETFDataObject['TotalAssetsUnderMgmt'] / 1000)
        ETFDataObject['SharesOutstanding'] = "{:,.0f}".format(ETFDataObject['SharesOutstanding'])
        ETFDataObject['InceptionDate'] = str(ETFDataObject['InceptionDate'])

        # List of columns we don't need
        for v in ['_id', 'DateOfScraping', 'ETFhomepage', 'holdings', 'FundHoldingsDate']:
            del ETFDataObject[v]

        ETFDataObject = pd.DataFrame(ETFDataObject, index=[0])
        ETFDataObject = ETFDataObject.replace(np.nan, 'nan', regex=True)
        ETFDataObject = ETFDataObject.loc[0].to_dict()

        allData = {}
        allData['ETFDataObject'] = ETFDataObject
        allData['HoldingsDatObject'] = HoldingsDatObject

        print(ETFDataObject)
        print(allData['HoldingsDatObject'])

        return allData

    except Exception as e:
        print("Issue in Flask app while fetching ETF Description Data")
        print(traceback.format_exc())
        return str(e)