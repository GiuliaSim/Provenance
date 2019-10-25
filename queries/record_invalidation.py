# The preprocessing method that deleted the record, $D_{i*}$.
# Input: D - dataframe

import pymongo
import pandas as pd
import pprint

def get_invalid_records():
	'''All $D_{*j}$ that were deleted'''
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

	return get_preprocessing_methods(invalid_records)

def get_preprocessing_methods(invalid_records):
	# Get the preprocessing methods that deleted the records
	out = entities.aggregate([ \
		{'$match': {'attributes.record_id':{'$in':invalid_records}}},
		{'$lookup': \
	    	{ \
	    		'from': 'relations', \
	    		'let': { 'entity': '$identifier'}, \
	    		'pipeline': [ \
	    			{ '$match': \
	    				{ '$expr': \
	    					{'$and': [\
		    					{ '$eq': [ '$prov:entity',  '$$entity' ] }, \
		    					{ '$eq': [ '$prov:relation_type',  'wasInvalidatedBy' ] } \
	    					]} \
	    				} \
	    			}, \
	    		], \
	    		'as': "invalidatedBy" \
	    	} \
	    }, \
		{'$group': {'_id': '$attributes.record_id', 'activities': {'$addToSet': '$invalidatedBy.prov:activity'}}}, \
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

	# Get preprocessing method that deleted the records:
	invalid_records = get_invalid_records()
	print('PREPROCESSING METHODS THAT DELETED RECORDS:')

	for r in invalid_records:
		pprint.pprint(r)
	
	# Close Mongodb connection:
	client.close()
