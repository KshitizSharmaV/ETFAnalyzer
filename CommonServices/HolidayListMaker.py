import sys  # Remove in production - KTZ
import traceback

# For Piyush System
sys.path.extend(['/home/piyush/Desktop/etf1903', '/home/piyush/Desktop/etf1903/ETFsList_Scripts',
                 '/home/piyush/Desktop/etf1903/HoldingsDataScripts',
                 '/home/piyush/Desktop/etf1903/CommonServices',
                 '/home/piyush/Desktop/etf1903/CalculateETFArbitrage'])
# For Production env
sys.path.extend(['/home/ubuntu/ETFAnalysis', '/home/ubuntu/ETFAnalysis/ETFsList_Scripts',
                 '/home/ubuntu/ETFAnalysis/HoldingsDataScripts', '/home/ubuntu/ETFAnalysis/CommonServices',
                 '/home/ubuntu/ETFAnalysis/CalculateETFArbitrage'])
sys.path.append("..")  # Remove in production - KTZ
import json
import codecs

class HolidayLister():
    def __init__(self, year):
        with codecs.open("CommonServices/Holidays"+year+".json", 'r', 'utf-8-sig') as fp:
            self.holidaysdict = json.load(fp)

        self.holidayslist = self.holidaysdict['response']['holidays']

        self.nationalholidaylist = [nh['date']['iso'] for nh in self.holidayslist if "National holiday" in nh['type']]
        # print("National Holiday List: {}".format(self.nationalholidaylist))
        self.christianholidaylist = [nh['date']['iso'] for nh in self.holidayslist if "Christian" in nh['type']]
        # print("Christian Holiday List: {}".format(self.christianholidaylist))
        self.observanceholidaylist = [nh['date']['iso'] for nh in self.holidayslist if "Observance" in nh['type']]
        # print("Observance Holiday List: {}".format(self.observanceholidaylist))
        self.localholidaylist = [nh['date']['iso'] for nh in self.holidayslist if "Local holiday" in nh['type'] and ("All" in nh['locations'] or "NY" in nh['locations'])]
        # print("Local(All states or NY) Holiday List: {}".format(self.localholidaylist))

    def getNationalHolidayList(self):
        return self.nationalholidaylist

    def getChristianHolidayList(self):
        return self.christianholidaylist

    def getObservanceHolidayList(self):
        return self.observanceholidaylist

    def getLocalHolidayList(self):
        return self.localholidaylist