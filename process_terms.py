##import all required packages 
import pymongo
import argparse
from pymongo import MongoClient
import sys
from datetime import datetime

##connect to debbie mongodb
#database_url = os.environ.get('DATABASE_URL')

parser = argparse.ArgumentParser()
parser.add_argument('-db', help= 'Database url. The mongo connection.')
parser.add_argument('-j', help= 'Collection prefix.')
args = parser.parse_args()
if (args.db == None):
	print("Please set database url parameter")
	sys.exit(1)
if (args.j == None):
	print("Please set the collection prefix. Example test, dev, or production (production environment lookout).")
	sys.exit(1)

database_url = args.db
client = MongoClient(database_url, maxPoolSize=None)
db = client ['DEBBIE']
collection_prefix = args.j
# assign the collection as 'annotations'
terms = db[collection_prefix+'_terms']
annotations = db[collection_prefix+'_annotations']
terms_by_years = db[collection_prefix+'_terms_by_years']
execution = db[collection_prefix+'_execution']
def process():
	print ("process starts")
	
	x = terms.delete_many({})
	print(x.deleted_count, " terms deleted.") 
	
	
	print("terms inserts process starts.") 
	annotations.aggregate([{'$project':
	{"text":"$annotations.lemma"}
	},
	{"$unwind": "$text" },
	{"$group": { "_id": "$text", "total": { "$sum": 1 }}},
    {"$sort":{"total":-1}}, { "$merge": { "into": collection_prefix+"_terms" } }
	])
	count = db[collection_prefix+'_terms'].count()
	print("terms inserts process ends. total inserted: " + str(count))
	
	x = terms_by_years.delete_many({})
	print(x.deleted_count, " terms deleted.") 
	
	print("year precalculations by term inserts starts")
	annotations.aggregate([
				{'$project':{"pmid":1, "title":1, "study_type":1,'text':"$annotations.lemma", 'pubdate':1,  
					'yearSubstring': { '$substr': [ "$pubdate", 0, 4 ] }}},
		{"$unwind": "$text" },
				{"$group": { "_id": {"text":"$text", "pmid":"$pmid", "year":"$yearSubstring"}}},
				
				{"$group": { "_id": {"text":"$_id.text", "year":"$_id.year"}, 
				"info": { "$addToSet": {"pmid":"$_id.pmid"}}, 'count':{'$sum':1}}},
				
				{'$project':{'text':'$_id.text', 'year':'$_id.year', 'count':1 , "info_2":{ "pmids":"$info"}, '_id':0}},
				{"$group": { "_id": { "text":"$text"}, "years": { "$addToSet": {"year":"$year","count":"$count"}}}},
				
				{ "$merge": { "into": collection_prefix+"_terms_by_years" } }
				], allowDiskUse=True)
	count2 = db[collection_prefix+'_terms_by_years'].count()
	print("year precalculations by term inserts ends. total inserted: " + str(count2))
	
	# Creating Dictionary of records to be
	# inserted
	d = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
	record = { "date": d, "comment": "DEBBIE terms preprocessed","terms_processed": count}
	execution.insert_one(record)
	
	print ("process end")
	
process()
