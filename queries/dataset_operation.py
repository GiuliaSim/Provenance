# Set of preprocessing methods that were applied to feature $D_{*j}$.
# Input: D_{*j}$

import pymongo
import pandas as pd
import pprint


def get_dataset_operation(activities, feature_name):
	return activities.find({'attributes.features_name': {'$regex': '.*' + feature_name + '*.'}})


if __name__ == "__main__":

	# Connect with MongoClient on the default host and port:
	client = pymongo.MongoClient('localhost', 27017)

	# Getting a Database:
	db = client['german_prov']

	# Get activities mongodb collection:
	activities = db.activities

	# Feature name of $D_{*j}$:
	feature_name = 'checking'

	# Get the activities that were applied to feature:
	methods = get_dataset_operation(activities, feature_name)

	# Print description of input entities and preprocessing methods that created the element $d_{ij}$:
	pprint.pprint(methods.explain())

	for act in methods:
		pprint.pprint(act)
