# db_manager.py
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

class DBManager:
    def __init__(self, uri, db_name, collection_name):
        self.mongo_handler = MongoDBHandler(uri, db_name, collection_name)

    def save_table(self, data):
        try:
            self.mongo_handler.save_table(data)
            return {"status": "success", "message": "Table data saved successfully"}
        except Exception as e:
            return {"status": "error", "message": str(e)}

    def get_table(self):
        try:
            data = self.mongo_handler.get_table()
            return {"status": "success", "data": data}
        except Exception as e:
            return {"status": "error", "message": str(e)}

    def save_merged_cells(self, merged_cells):
        try:
            self.mongo_handler.save_merged_cells(merged_cells)
            return {"status": "success", "message": "Merged cells data saved successfully"}
        except Exception as e:
            return {"status": "error", "message": str(e)}

    def get_merged_cells(self):
        try:
            merged_cells = self.mongo_handler.get_merged_cells()
            return {"status": "success", "data": merged_cells}
        except Exception as e:
            return {"status": "error", "message": str(e)}

    def append_table(self, data):
        try:
            self.mongo_handler.append_table(data)
            return {"status": "success", "message": "Table data appended successfully"}
        except Exception as e:
            return {"status": "error", "message": str(e)}
