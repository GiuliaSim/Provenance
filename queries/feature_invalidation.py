# The preprocessing method that deleted the feature, $D_{*j}$.
# Input: D - dataframe

import pymongo
import pandas as pd
import pprint

def get_invalid_features():
	'''All $D_{*j}$ that were deleted'''
	# Get invalidated entities id:
	invalid_ents_id = relations.find({'prov:relation_type': 'wasInvalidatedBy'}, {'prov:entity': 1, '_id': 0}).distinct('prov:entity')
	
	# Group entities by feature_name
	features = entities.aggregate([ \
		{'$group': {'_id': '$attributes.feature_name', 'entities': {'$addToSet': '$identifier'}}} \
	])

	# Get deleted features
	# If all feature entities are invalidated, the feature is deleted
	invalid_features = []
	for feature in features:
		is_deleted = True
		ents = feature['entities']
		feature_name = feature['_id']
		for ent in ents:
			if ent not in invalid_ents_id:
				is_deleted = False
				break
		if is_deleted:
			invalid_features.append(feature_name)
	
	return get_preprocessing_methods(invalid_features)


def get_preprocessing_methods(invalid_features):
	# Get the preprocessing methods that deleted the features
	out = activities.aggregate([ \
		{'$match': {'attributes.features_name':{'$in':invalid_features}}},
		{'$sort': {'attributes.operation_number':-1}},
		{'$group': {'_id': '$attributes.features_name', 'function_name': {'$first': '$attributes.function_name'}}}, \
		{'$project': {'_id':0,'feature_name':'$_id', 'function_name':1}} \
	])
	return out


if __name__ == "__main__":

	# Connect with MongoClient on the default host and port:
	client = pymongo.MongoClient('localhost', 27017)

	# Getting a Database:
	db = client['german_prov']

	# Get entities, activities and relations mongodb collection:
	entities = db.entities
	activities = db.activities
	relations = db.relations

	# Get preprocessing method that deleted the features:
	invalid_features = get_invalid_features()
	print('PREPROCESSING METHODS THAT DELETED FEATURES:')

	for f in invalid_features:
		pprint.pprint(f)
	
	# Close Mongodb connection:
	client.close()
