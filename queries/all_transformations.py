# Set of preprocessing methods applied to $D$, and the features, $D_{*j}$, they affect.
# Input: D - dataframe

import pymongo
import pandas as pd


def all_transformations(db):
	activities = db.activities

	all_act = {}
	for act in activities.find():
		function_name = act['attributes']['function_name']
		all_act.setdefault(function_name,[]).append(act['attributes']['features_name'])

	for k, v in all_act.items():
		print(k, ":", v)

if __name__ == "__main__":

	#data = pd.read_csv('../results/GermanCredit_prov/german_onehot.csv') 
	#columns = data.columns
	#index = data.index

	client = pymongo.MongoClient('localhost', 27017)
	db = client['german_prov']
	all_transformations(db)
