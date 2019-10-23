# Output entities.

import pymongo
#from pyspark.sql import SparkSession
import pandas as pd
import pprint


if __name__ == "__main__":

	data = pd.read_csv('../results/GermanCredit_prov/german_onehot.csv') 
	columns = data.columns
	m, n = data.shape
	
	print(m, n)

	client = pymongo.MongoClient('localhost', 27017)
	db = client['german_prov']

	entities = db.entities
	relations = db.relations

	out = entities.aggregate([
		{'$sort': {'attributes.instance':-1}}, \
		{'$group': { \
			'_id': '$identifier', \
			'record_id': {'$first': '$attributes.record_id'}, \
			'value': {'$first': '$attributes.value'}, \
			'feature_name': {'$first': '$attributes.feature_name'}, \
			'index': {'$first': '$attributes.index'}, \
			'instance': {'$first': '$attributes.instance'} \
		}}, \
	    #{'$group': {'_id': '$identifier', 'instance':{'$max':'$attributes.instance'}}}, \
	    {'$lookup': \
	    	{ \
	    		'from': 'relations', \
	    		'let': { 'entity': '$_id', 'relation_type': '$relation_type' }, \
	    		'pipeline': [ \
	    			{ '$match': \
	    				{ '$expr': \
	    					{ '$and': \
	    						[ \
	    							{ '$eq': ['$$relation_type', 'wasInvalidatedBy']}, \
	    							{ '$eq': [ '$prov:entity',  '$$entity' ] } \
	    						] \
	    					} \
	    				} \
	    			}, \
	    			#{ $project: { stock_item: 0, _id: 0 } } \
	    		], \
	    		'as': "invalidated" \
	    	} \
	    }, \
		{'$match': \
	    	{ \
	    		'invalidated': [] \
	    	} \
	    }, \
	    {'$out': 'outputs'}
	])

	print('Done')

	a=0
	for doc in out:
		if(a<3):
			pprint.pprint(doc)
			a+=1
		else:
			break
	
	client.close()


