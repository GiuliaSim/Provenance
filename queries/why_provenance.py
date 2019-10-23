# The inputs that influenced $d_{ij}$.
# Input: $d_{ij}$ - single element

import pymongo
#from pyspark.sql import SparkSession
import pandas as pd
import pprint


def get_why_prov(relations, entities):
	acts = []	
	generated_act = relations.find({'prov:entity': {'$in':entities}, 'prov:relation_type':'wasGeneratedBy'})
	for act in generated_act:
		act_id = act['prov:activity']
		if act_id not in acts:
			acts.append(act_id)

	ents = []
	used_ent = relations.find({'prov:activity':{'$in':acts}, 'prov:relation_type':'used'})
	for ent in used_ent:
		ent_id = ent['prov:entity']
		if ent_id not in ents:
			ents.append(ent_id)

	if not acts:
		return entities
	else:
		return get_why_prov(relations, ents)

	





if __name__ == "__main__":

	#data = pd.read_csv('../results/GermanCredit_prov/german_onehot.csv') 
	#columns = data.columns
	#m, n = data.shape
	#print(m, n)

	entity_id = 'entity:0d69c672-d521-498e-aa94-7773f634fb39'
	

	client = pymongo.MongoClient('localhost', 27017)
	db = client['german_prov']

	entities = db.entities
	relations = db.relations

	ents = get_why_prov(relations, [entity_id])

	why_prov = entities.find({'identifier':{'$in':ents}})

	print(why_prov.explain())




