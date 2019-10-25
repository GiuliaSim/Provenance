# Output entities.

import pymongo
import pandas as pd
import pprint


def get_outputs(db):

	# Get entities and relations mongodb collection:
	entities = db.entities
	relations = db.relations

	# Get invalidated entities id
	invalidated_ents_id = relations.find({'prov:relation_type': 'wasInvalidatedBy'}, {'prov:entity': 1, '_id': 0}).distinct('prov:entity')

	# Get output entities and save in outputs collection:
	output = entities.aggregate([
		{'$match': {'identifier': {'$nin': invalidated_ents_id}}},
	    {'$out': 'outputs'}
	])

	print('Output entities saved in outputs mongodb collection.')


if __name__ == "__main__":

	#data = pd.read_csv('../results/GermanCredit_prov/german_onehot.csv') 
	#columns = data.columns
	#m, n = data.shape
	#print(m, n)

	# Connect with MongoClient on the default host and port:
	client = pymongo.MongoClient('localhost', 27017)
	
	# Getting a Database:
	db = client['german_prov']

	get_outputs(db)
	
	# Close Mongodb connection:
	client.close()
