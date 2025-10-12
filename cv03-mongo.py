from init import collection
from pprint import pprint
from datetime import datetime

def separate(text):
    print(f'---------------------------{text}---------------------------')

separate("Jeden dokument")
print(collection.find_one())
separate("Celkovy pocet")
print(collection.count_documents({}))
separate("Prumer fotek na clanek")
result = collection.aggregate([
    {"$group": {"_id": None, "avgPhotos": {"$avg": "$images"}}}
])
print(next(result)["avgPhotos"])
separate("Pocet clanku s vice nez 100 komentari")
print(collection.count_documents({"disc": {"$gt": 100}}))
separate("Kategorie za 2022")

pipeline = [
    {"$match": {"date": {"$regex": "^2022"}}},
    {"$unwind": "$tags"},
    {"$group": {"_id": "$tags", "count": {"$sum": 1}}},
    {"$sort": {"count": -1}},
    {"$limit": 20}  
]
for c in collection.aggregate(pipeline):
    print(c)