import csv
import os
import pandas as pd
import glob
import re


def main():

	path = 'C:/Users/gatsuuubii/Documents/Engineering Test Risk Analytics/Engineering Test Files/'
	result = combine(path)
	
	resultFile = path + 'Combined.csv'
	result.to_csv(resultFile, index = False)

def combine(path):
	all_files = glob.glob(path + "/*.csv")

	temp = []

	for filename in all_files:
		with open(filename, 'r') as file:
			reader = csv.reader(file)
			baseName = re.sub('.csv', '', os.path.basename(file.name))

		if baseName != 'Combined':
			df = pd.read_csv(filename, index_col=None, header=0)
			baseNameCleaned = re.sub(r' \d', '', baseName)
			temp.append(df['Source IP'].to_frame().assign(Environment = baseNameCleaned))

	result = pd.concat(temp, axis=0, ignore_index=True).drop_duplicates()

	return result

main()
