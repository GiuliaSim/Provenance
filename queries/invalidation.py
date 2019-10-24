# Set of all $D_{i*}$, $D_{*j}$, $d_{ij}$ that were deleted.
# Input: D - dataframe

import pymongo
import pandas as pd
import pprint

def get_invalid_items(relations, entities):
	'''All $d_{ij}$ that were deleted'''
	ents_id = relations.find({'prov:relation_type': 'wasInvalidatedBy'}, {'prov:entity': 1, '_id': 0})
	invalid_ents = entities.find({'identifier': {'$in': ents_id}})
	return invalid_ents

def get_invalid_features(activities):
	'''All $D_{*j}$ that were deleted'''
	# Get invalidated entities id:
	invalid_ents_id = relations.find({'prov:relation_type': 'wasInvalidatedBy'}, {'prov:entity': 1, '_id': 0}).distinct('prov:entity')
	
	# Group entities by feature_name
	features = entities.aggregate([ \
		{'$group': {'_id': '$attributes.feature_name', 'entities': {'$addToSet': '$identifier'}}} \
	])

	# Get deleted records
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

	return invalid_features


def get_invalid_records(relations, entities):
	'''All $D_{i*}$ that were deleted'''
	# Get invalidated entities id:
	invalid_ents_id = relations.find({'prov:relation_type': 'wasInvalidatedBy'}, {'prov:entity': 1, '_id': 0}).distinct('prov:entity')
	
	# Group entities by record_id
	records = entities.aggregate([ \
		{'$group': {'_id': '$attributes.record_id', 'entities': {'$addToSet': '$identifier'}}} \
	])

	# Get deleted records
	# If all record entities are invalidated, the record is deleted
	invalid_records = []
	for record in records:
		is_deleted = True
		ents = record['entities']
		record_id = record['_id']
		for ent in ents:
			if ent not in invalid_ents_id:
				is_deleted = False
				break
		if is_deleted:
			invalid_records.append(record_id)

	return invalid_records

if __name__ == "__main__":

	# Connect with MongoClient on the default host and port:
	client = pymongo.MongoClient('localhost', 27017)

	# Getting a Database:
	db = client['german_prov']

	# Get entities, activities and relations mongodb collection:
	entities = db.entities
	activities = db.activities
	relations = db.relations

	# Get the entities that were deleted:
	invalid_ents = get_invalid_items(relations, entities)

	# Get the features name that were deleted:
	invalid_features = get_invalid_features(activities)
	print('DELETED FEATURES NAME:')
	pprint.pprint(invalid_features)

	# Get the record_id that were deleted:
	invalid_records = get_invalid_records(relations, entities)
	print('DELETED RECORDS ID:')
	pprint.pprint(invalid_records)
