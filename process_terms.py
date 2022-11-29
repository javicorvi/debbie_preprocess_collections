##import all required packages 
import pymongo
from pymongo import MongoClient
from dotenv import load_dotenv
import os
from datetime import datetime

load_dotenv()

##connect to debbie mongodb
#database_url = os.environ.get('DATABASE_URL')

database_url = os.environ.get('DATABASE_URL')
client = MongoClient(database_url, maxPoolSize=None)
db = client ['DEBBIE']

# assign the collection as 'annotations'
terms = db.production_terms
annotations = db.production_annotations
production_terms_by_years = db.production_terms_by_years
production_execution = db.production_execution
def process():
	print ("process starts")
	
	x = terms.delete_many({})
	print(x.deleted_count, " terms deleted.") 
	
	
	print("terms inserts process starts.") 
	result_terms = annotations.aggregate([{'$project':
	{"text":"$annotations.lemma"}
	},
	{"$unwind": "$text" },
	{"$group": { "_id": "$text", "total": { "$sum": 1 }}},
    {"$sort":{"total":-1}}, { "$merge": { "into": "production_terms" } }
	])
	count = db.production_terms.count()
	print("terms inserts process ends. total inserted: " + str(count))
	
	x = production_terms_by_years.delete_many({})
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
				
				{ "$merge": { "into": "production_terms_by_years" } }
				], allowDiskUse=True)
	count2 = db.production_terms.count()
	print("year precalculations by term inserts ends. total inserted: " + str(count2))
	
	
	# Creating Dictionary of records to be
	# inserted
	d = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
	record = { "date": d, "comment": "DEBBIE terms preprocessed","terms_processed": count}
	production_execution.insert_one(record)
	
	print ("process end")
	
	
	
process()
