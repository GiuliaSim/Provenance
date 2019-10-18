# The inputs that influenced $d_{ij}$.
# Input: $d_{ij}$ - single element

import pymongo
#from pyspark.sql import SparkSession
import pandas as pd


if __name__ == "__main__":

	data = pd.read_csv('../results/GermanCredit_prov/german_onehot.csv') 
	columns = data.columns
	m, n = data.shape
	
	print(m, n)

	client = pymongo.MongoClient('localhost', 27017)
	db = client['german_prov']

	entities = db.entities
	activities = db.activities
	relations = db.relations

	# Get entities with max instance number. (Last entities)
	# last_entities = entities.aggregate([ \
	#     {'$sort': {'attributes.instance':-1}}, \
	#     {'$group': {'_id': '$identifier'}} \
	#     #{'$group': {'_id': '$identifier', 'instance': {'$first': '$attributes.instance'}, '_id_seen': {'$first': '$_id'}}} \
	# ])

	#last_entities = list(last_entities)

	#print(last_entities[0])
	#print(len(last_entities))

	# Get wasInvalidatedBy relations
	#invalidated_rel = relations.find({'relation_type':'wasInvalidatedBy'})


	# out = entities.aggregate([ \
	# 	{'$sort': {'attributes.instance':-1}}, \
	#     {'$group': {'_id': '$identifier'}}, \
	# 	{'$lookup': \
	# 	            { \
	# 	                'from': 'relations', \
	# 	                'localField': 'identifier', \
	# 	                'foreignField': 'prov:entity', \
	# 	                'as': 'invalidated' \
	# 	            } \
	# 	}, \
	# 	{ '$match': { 'invalidated.relation_type': 'wasInvalidatedBy' } }
	# ]) \


	out = entities.aggregate([
		#{'$sort': {'attributes.instance':-1}}, \
	    #{'$group': {'_id': '$identifier'}}, \
	    {'$lookup': \
	    	{ \
	    		'from': 'relations', \
	    		'let': { 'entity': "$identifier"}, \
	    		'pipeline': [ \
	    			{ '$match': \
	    				{ '$expr': \
	    					{ '$and': \
	    						[ \
	    							{ 'relation_type': 'wasInvalidatedBy'}, \
	    							{ '$eq': [ "$prov:entity",  "$$entity" ] } \
	    						] \
	    					} \
	    				} \
	    			}, \
	    			#{ $project: { stock_item: 0, _id: 0 } } \
	    		], \
	    		'as': "invalidated" \
	    	} \
	    } \
	])

	print('Done')

	out = list(out)
	print(out[0])
	print(len(out))
	client.close()


