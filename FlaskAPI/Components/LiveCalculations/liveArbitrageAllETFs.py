import pandas as pd

class LiveArbitrageAllETFs(object):

	def __init__(self):
		allETFsList =pd.read_csv('./CalculateETFArbitrage/final365list.csv')
		ETFDBCategory = allETFsList['ETFdb.com Category']

	def ETFDbCategoryList()