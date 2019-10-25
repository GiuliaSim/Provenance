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

	# Connect with MongoClient on the default host and port:
	client = pymongo.MongoClient('localhost', 27017)
	
	# Getting a Database:
	db = client['german_prov']
	
	all_transformations(db)

	# Close Mongodb connection:
	client.close()
