# The inputs and preprocessing methods that created $d_{ij}$
# Input:  $d_{ij}$

import pymongo
import pandas as pd
import pprint


def get_how_prov(relations, entities, ents_id):
	# Get input entities from ents_id:
	input_entities = entities.find({'identifier':{'$in': ents_id}, 'attributes.instance':'-1'},{'identifier':1,'_id':0}).distinct('identifier')
	
	# Select intermediate entities:
	diff = lambda l1, l2: [x for x in l1 if x not in l2]
	ents_id = diff(ents_id, input_entities)
	
	# Find the activities that generated the ents_id:
	acts = []	
	generated_act = relations.find({'prov:entity': {'$in':ents_id}, 'prov:relation_type':'wasGeneratedBy'})
	for act in generated_act:
		act_id = act['prov:activity']
		if act_id not in acts:
			acts.append(act_id)

	if not acts:
		return (input_entities, acts)
	else:
		# Find the entities used by the activities:
		ents = []
		used_ent = relations.find({'prov:activity':{'$in':acts}, 'prov:relation_type':'used'})
		for ent in used_ent:
			ent_id = ent['prov:entity']
			if ent_id not in ents:
				ents.append(ent_id)

		e, a = get_how_prov(relations, ents)
		return (input_entities + e, acts + a)


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

	# Get the inputs ids and activities id that created an element:
	ents, acts = get_how_prov(relations, [entity_id])

	# Find mongodb documents from identifier list:
	why_prov = entities.find({'identifier':{'$in':ents}})
	methods = activities.find({'identifier':{'$in':acts}})

	#for m in methods:
	#	print(m)

	# Print description of input entities and preprocessing methods that created the element $d_{ij}$:
	print(why_prov.explain())
	print(methods.explain())
