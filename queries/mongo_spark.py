import pymongo
from pymongo import MongoClient
from pyspark.sql import SparkSession


if __name__ == "__main__":
	spark = SparkSession \
	    .builder \
	    .appName('german_prov') \
	    .config('spark.mongodb.input.uri', 'mongodb://127.0.0.1/german_prov.entities') \
	    .config('spark.jars.packages','org.mongodb.spark:mongo-spark-connector_2.11:2.2.0') \
	    .getOrCreate()

	entities = spark.read.format('com.mongodb.spark.sql.DefaultSource').load()
	activities = spark.read.format('com.mongodb.spark.sql.DefaultSource').option('uri','mongodb://127.0.0.1/german_prov.activities').load()
	relations = spark.read.format('com.mongodb.spark.sql.DefaultSource').option('uri','mongodb://127.0.0.1/german_prov.relations').load()

	entities.printSchema()
	activities.printSchema()
	relations.printSchema()

	all_acts = activities.rdd \
		.map(lambda x: (x.attributes.function_name, x.attributes.features_name)) \
		.reduceByKey(lambda a,b: a+', '+b)

	all_acts.toDF().show(10, False)

	rels = relations.rdd \
		.filter(lambda x: x['prov:relation_type'] == 'wasInvalidatedBy') \
		.map(lambda x: (x['prov:entity'], x['prov:activity']))

	ents = entities.rdd \
		.map(lambda x: (x.identifier, (x.attributes.record_id, x.attributes.feature_name, x.attributes.value)))

	#join = relations.filter(relations['prov:relation_type'] == 'wasInvalidatedBy') \
	#	.join(entities, entities.identifier == relations['prov:entity'], 'inner')

	join = rels.join(ents) \
		.map(lambda x: (x[1][0], (x[0], x[1][1])))

	acts = activities.rdd \
		.map(lambda x : (x.identifier, (x.attributes.function_name, x.attributes.features_name)))

	join2 = join.join(acts)



	join2.toDF().show(10, False)

	# df = entities.rdd \
	# 	.map(lambda x: (x.identifier, 1)) \
	# 	.reduceByKey(lambda a,b: a+b) \
	# 	.sortBy(lambda x: x[1], ascending=True)

	# df.toDF().show(10, False)


	#client = MongoClient('localhost', 27017)
	#db = client['german_prov']
	#entities = db.entities
	#activities = db.activities
	#relations = db.relations

	#print(entities.count_documents({}))
