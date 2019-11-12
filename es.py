#!/usr/bin/python

from elasticsearch import Elasticsearch
from elasticsearch import helpers
import pandas as pd
import sys

filepath = '/home/abdulsalam/healthframe.csv'

def main():

	es = Elasticsearch(http_compress=True)
	df = pd.read_csv(filepath)
	#print(df)	
	df_iter = df.iterrows()
	index, document = next(df_iter)

	use_these_keys = ['Coinsurance',
				  'Deductible',
				  'Group',
				  'Payment',
				  'Policy limit',
				  'Type of disease',
				  'Year',
				  'Subject',
				  'Number of claims'
				 ]

	filterKeys(document, use_these_keys)		 
				 	
def filterKeys(document,use_these_keys):
	return {key: document[key] for key in use_these_keys }

	helpers.bulk(es, doc_generator(frame))    

def doc_generator(innerjoin):
	df_iter = df.iterrows()
	for index, document in df_iter:
		try:
			yield {
			"_index": 'health_index',
			"_type": "_doc",
			"_source": filterKeys(document),
			}
		except StopIteration:
			return

if __name__ == "__main__":
   main()
   #print("len :",len(sys.argv))