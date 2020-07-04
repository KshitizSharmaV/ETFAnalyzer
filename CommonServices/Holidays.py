from pandas.tseries.holiday import USFederalHolidayCalendar
import datetime
from pandas.tseries.offsets import BDay


def HolidayCheck(dateToCheck):
	cal=USFederalHolidayCalendar()
	currentYear = dateToCheck.year
	startDate = str(currentYear)+'-01-01'
	endDate = str(currentYear+1)+'-01-01'
	
	# holidays=cal.holidays(start='2020-01-01', end='2021-01-01').to_pydatetime()
	Federalholidays=cal.holidays(start=startDate, end=endDate).to_pydatetime()
	Federalholidays  = [x.date() for x in Federalholidays]
	
	if dateToCheck in Federalholidays:
		return True
	else:
		weekno = dateToCheck.weekday()
		return True if weekno>=5 else False
	
def LastWorkingDay(dateToCheck):
    while HolidayCheck(dateToCheck):
        dateToCheck = dateToCheck - datetime.timedelta(days=1)
    return datetime.datetime.combine(dateToCheck, datetime.datetime.min.time())

def isTimeBetween(begin_time=None, end_time=None, check_time=None):
	# If check time is not given, default to current UTC time
	check_time = check_time or datetime.estnow().time()
	if not end_time:
		return check_time >= begin_time
	else:
		if begin_time < end_time:
			return check_time >= begin_time and check_time <= end_time
		else: # crosses midnight
			return check_time >= begin_time or check_time <= end_time