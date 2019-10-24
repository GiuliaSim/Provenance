# Set of preprocessing methods that were applied to feature, $D_{*j}$.
# Input: $d_{ij}$

import pymongo
import pandas as pd
import pprint

def get_item_operation(relations, entity_id):
	acts = relations.find({'prov:entity': {'$in': entity_id}}, {'prov:activity': 1, '_id': 0}).distinct('prov:activity')
	used_ents = relations.find({'prov:generatedEntity': {'$in': entity_id}}, {'prov:usedEntity': 1, '_id': 0}).distinct('prov:usedEntity')
	if used_ents:
		return acts + get_item_operation(relations, used_ents)
	else:
		return acts


if __name__ == "__main__":

	# Connect with MongoClient on the default host and port:
	client = pymongo.MongoClient('localhost', 27017)

	# Getting a Database:
	db = client['german_prov']

	# Get entities, activities and relations mongodb collection:
	entities = db.entities
	activities = db.activities
	relations = db.relations

	# Element identifier $d_{ij}$:
	entity_id = 'entity:0d69c672-d521-498e-aa94-7773f634fb39'

	# Get the activities id that were applied to element:
	acts = get_item_operation(relations, [entity_id])

	# Find mongodb documents from identifier list:
	methods = activities.find({'identifier':{'$in':acts}})

	for m in methods:
		pprint.pprint(m)

	# Print description of input entities and preprocessing methods that created the element $d_{ij}$:
	#pprint.pprint(methods.explain())
