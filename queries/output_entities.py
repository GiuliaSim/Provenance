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
	    {'$lookup': \
	    	{ \
	    		'from': 'relations', \
	    		'let': { 'entity': '$identifier'}, \
	    		'pipeline': [ \
	    			{ '$match': \
	    				{ '$expr': \
	    					{ '$or': [ \
	    						{'$and': \
		    						[ \
		    							{ '$eq': ['$relation_type', 'wasInvalidatedBy']}, \
		    							{ '$eq': [ '$prov:entity',  '$$entity' ] } \
		    						] \
		    					}, \
		    					{'$and': \
		    						[ \
		    							{ '$eq': ['$relation_type', 'wasDerivedFrom']}, \
		    							{ '$eq': [ '$prov:usedEntity',  '$$entity' ] } \
		    						] \
		    					} \

	    					]} \
	    				} \
	    			}, \
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


