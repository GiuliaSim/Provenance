# The preprocessing method that deleted the item, $d_{ij}$.
# Input: D - dataframe

import pymongo
import pandas as pd
import pprint

def get_preprocessing_methods():
	invalid_items = relations.aggregate([ \
		{'$match': {'prov:relation_type':'wasInvalidatedBy'}},
		{'$lookup': \
	    	{ \
	    		'from': 'activities', \
	    		'let': { 'activity': '$prov:activity'}, \
	    		'pipeline': [ \
	    			{ '$match': \
	    				{ '$expr': \
		    				{ '$eq': [ '$identifier',  '$$activity' ] }, \
	    				} \
	    			}, \
	    		], \
	    		'as': "activity" \
	    	} \
	    }, \
		{'$project': {'_id': 0, 'entity_id':'$prov:entity', 'preprocessing_method':'$activity.attributes.function_name'}}, \
		#{'$group': {'_id': '$preprocessing_method', 'invalidated_entities': {'$addToSet': '$entity_id'}}} \
	])

	return invalid_items

if __name__ == "__main__":

	# Connect with MongoClient on the default host and port:
	client = pymongo.MongoClient('localhost', 27017)

	# Getting a Database:
	db = client['german_prov']

	# Get entities, activities and relations mongodb collection:
	entities = db.entities
	activities = db.activities
	relations = db.relations

	# Get preprocessing method that deleted the items:
	invalid_items = get_preprocessing_methods()
	print('PREPROCESSING METHODS THAT DELETED ITEMS:')

	for i in invalid_items:
		pprint.pprint(i)

	# Close Mongodb connection:
	client.close()
