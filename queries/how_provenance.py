# The inputs and preprocessing methods that created $d_{ij}$
# Input:  $d_{ij}$

import pymongo
import pandas as pd
import pprint

def get_how_prov(ents_id):
	# Get input entities from ents_id:
	input_entities = entities.find({'identifier':{'$in': ents_id}, 'attributes.instance':'-1'},{'identifier':1,'_id':0}).distinct('identifier')
	
	# Select intermediate entities:
	diff = lambda l1, l2: [x for x in l1 if x not in l2]
	ents_id = diff(ents_id, input_entities)
	
	# Find the activities that generated the ents_id:
	generated_act = relations.find({'prov:entity': {'$in':ents_id}, 'prov:relation_type':'wasGeneratedBy'}).distinct('prov:activity')

	if not generated_act:
		return (input_entities, generated_act)
	else:
		# Find the entities used by the activities:
		used_ent = relations.find({'prov:activity':{'$in':generated_act}, 'prov:relation_type':'used'}).distinct('prov:entity')

		e, a = get_how_prov(used_ent)
		return (input_entities + e, generated_act + a)

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
	entity_id = 'entity:80a19bef-6fc4-4ce8-bac4-455afdf4abb0'

	# Get the inputs ids and activities id that created an element:
	ents, acts = get_how_prov([entity_id])

	# Find mongodb documents from identifier list:
	why_prov = entities.find({'identifier':{'$in':ents}})
	methods = activities.find({'identifier':{'$in':acts}})

	for m in methods:
		pprint.pprint(m)

	# Print description of input entities and preprocessing methods that created the element $d_{ij}$:
	#pprint.pprint(why_prov.explain())
	#pprint.pprint(methods.explain())

	# Close Mongodb connection:
	client.close()
