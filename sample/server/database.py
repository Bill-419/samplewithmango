from pymongo import MongoClient

class MongoDBHandler:
    def __init__(self, uri, db_name, collection_name):
        self.client = MongoClient(uri)
        self.db = self.client[db_name]
        self.collection = self.db[collection_name]

    def save_table(self, data):
        self.collection.delete_many({})
        self.collection.insert_many(data)

    def get_table(self):
        return list(self.collection.find({}, {"_id": 0}))

    def save_merged_cells(self, merged_cells):
        self.collection.update_one({"type": "merged_cells"}, {"$set": {"merged_cells": merged_cells}}, upsert=True)

    def get_merged_cells(self):
        result = self.collection.find_one({"type": "merged_cells"})
        return result["merged_cells"] if result else []
        
    def append_table(self, data):
        self.collection.insert_many(data)