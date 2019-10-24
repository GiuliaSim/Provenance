# Set of preprocessing methods that were applied to record, $D_{i*}$.
# Input: $D_{i*}$

import pymongo
import pandas as pd
import pprint

def get_record_operation(entities, record_id):
	# Get list of entities id related to the record:
	#ents = entities.find({'attributes.record_id':record_id}, {'identifier':1,'_id':0}).distinct('identifier')

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
	    {'$project': {'relations.prov:activity': 1, '_id': 0}} \
	])

	acts = []
	for elem in out:
		for act in elem['relations']:
			act_id = act['prov:activity']
			acts.append(act_id)

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
	record_id = 'b5560de6-226c-4ea5-aeb6-6213e462ec5b'

	# Get the activities id that were applied to record_id:
	acts = get_record_operation(entities, record_id)

	# Find mongodb documents from atcs list:
	methods = activities.find({'identifier':{'$in': acts}})

	for m in methods:
		pprint.pprint(m)

	# Print description of preprocessing methods that were applied to record, $D_{i*}$:
	#pprint.pprint(methods.explain())

