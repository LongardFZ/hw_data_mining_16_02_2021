import pymongo

db_client = pymongo.MongoClient()
db = db_client['data_mining_16_02_2021']
collection = db['magnit']
#{'promo_name': 'Скидка', 'title': {'$regex': 'Таблетки'}}
for itm in collection.find():
    print(itm)
