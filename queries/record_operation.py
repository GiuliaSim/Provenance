# Set of preprocessing methods that were applied to record, $D_{i*}$.
# Input: $D_{i*}$

import pymongo
import pandas as pd
import pprint

def get_record_operation(record_id):
	# Get list of activities id related to the record:
	out = entities.aggregate([
		{'$match': \
			{'attributes.record_id': record_id}
		}, \
	    {'$lookup': \
	    	{ \
	    		'from': 'relations', \
	    		'let': { 'entity': '$identifier'}, \
	    		'pipeline': [ \
	    			{ '$match': \
	    				{ '$expr': \
	    					{ '$eq': [ '$prov:entity',  '$$entity' ] }
	    				} \
	    			}, \
	    		], \
	    		'as': "relations" \
	    	} \
	    }, \
	    {'$project': {'activities': '$relations.prov:activity', '_id': 0}} \
	])

	acts = []
	for elem in out:
		acts += elem['activities']

	return acts

if __name__ == "__main__":

	# Connect with MongoClient on the default host and port:
	client = pymongo.MongoClient('localhost', 27017)

	# Getting a Database:
	db = client['german_prov']

	# Get entities and activities mongodb collection:
	entities = db.entities
	activities = db.activities

	# Record identifier $D_{i*}$:
	record_id = 'f8c0771d-8147-4dc9-bd66-b6119755effe'

	# Get the activities id that were applied to record_id:
	acts = get_record_operation(record_id)

	# Find mongodb documents from atcs list:
	methods = activities.find({'identifier':{'$in': acts}})

	for m in methods:
		pprint.pprint(m)

	# Print description of preprocessing methods that were applied to record, $D_{i*}$:
	#pprint.pprint(methods.explain())
	
	# Close Mongodb connection:
	client.close()
