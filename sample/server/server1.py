from flask import Flask, request, jsonify
from pymongo import MongoClient
import json
import time
from concurrent.futures import ThreadPoolExecutor
from readerwriterlock import rwlock

app = Flask(__name__)
executor = ThreadPoolExecutor(max_workers=40)  # 根据需求调整线程池大小

# 全局变量来存储 MongoDBHandler 实例
db_handlers = {}
lock = rwlock.RWLockFairD()

class MongoDBHandler:
    def __init__(self, uri, db_name, collection_name):
        self.client = MongoClient(uri)
        self.db = self.client[db_name]
        self.collection = self.db[collection_name]

    def save_table(self, data):
        start_time = time.time()
        with lock.gen_wlock():
            step_start_time = time.time()
            self.collection.delete_many({})
            step_end_time = time.time()
            print(f"Delete many took {step_end_time - step_start_time:.4f} seconds")

            step_start_time = time.time()
            self.collection.insert_many(data)
            step_end_time = time.time()
            print(f"Insert many took {step_end_time - step_start_time:.4f} seconds")
        end_time = time.time()
        print(f"save_table execution time: {end_time - start_time:.4f} seconds")

    def get_table(self):
        start_time = time.time()
        with lock.gen_rlock():
            step_start_time = time.time()
            result = list(self.collection.find({}, {"_id": 0}))
            step_end_time = time.time()
            print(f"Find took {step_end_time - step_start_time:.4f} seconds")
        end_time = time.time()
        print(f"get_table execution time: {end_time - start_time:.4f} seconds")
        return result

    def save_merged_cells(self, merged_cells):
        start_time = time.time()
        with lock.gen_wlock():
            step_start_time = time.time()
            self.collection.update_one({"type": "merged_cells"}, {"$set": {"merged_cells": merged_cells}}, upsert=True)
            step_end_time = time.time()
            print(f"Update one took {step_end_time - step_start_time:.4f} seconds")
        end_time = time.time()
        print(f"save_merged_cells execution time: {end_time - start_time:.4f} seconds")

    def get_merged_cells(self):
        start_time = time.time()
        with lock.gen_rlock():
            step_start_time = time.time()
            result = self.collection.find_one({"type": "merged_cells"})
            step_end_time = time.time()
            print(f"Find one took {step_end_time - step_start_time:.4f} seconds")
        end_time = time.time()
        print(f"get_merged_cells execution time: {end_time - start_time:.4f} seconds")
        return result["merged_cells"] if result else []

    def append_table(self, data):
        start_time = time.time()
        with lock.gen_wlock():
            step_start_time = time.time()
            self.collection.insert_many(data)
            step_end_time = time.time()
            print(f"Insert many took {step_end_time - step_start_time:.4f} seconds")
        end_time = time.time()
        print(f"append_table execution time: {end_time - start_time:.4f} seconds")

def run_async(func, *args):
    future = executor.submit(func, *args)
    return future.result()

def get_db_handler(uri, db_name, collection_name):
    key = (uri, db_name, collection_name)
    with lock.gen_wlock():
        if key not in db_handlers:
            db_handlers[key] = MongoDBHandler(uri, db_name, collection_name)
        return db_handlers[key]

@app.route('/save_table', methods=['POST'])
def save_table_route():
    data = request.json.get('data')
    uri = request.json.get('uri')
    db_name = request.json.get('db_name')
    collection_name = request.json.get('collection_name')
    db_handler = get_db_handler(uri, db_name, collection_name)
    run_async(db_handler.save_table, data)
    return jsonify({"status": "success"}), 200

@app.route('/get_table', methods=['POST'])
def get_table_route():
    uri = request.json.get('uri')
    db_name = request.json.get('db_name')
    collection_name = request.json.get('collection_name')
    db_handler = get_db_handler(uri, db_name, collection_name)
    result = run_async(db_handler.get_table)
    return jsonify({"status": "success", "data": result}), 200

@app.route('/save_merged_cells', methods=['POST'])
def save_merged_cells_route():
    merged_cells = request.json.get('merged_cells')
    uri = request.json.get('uri')
    db_name = request.json.get('db_name')
    collection_name = request.json.get('collection_name')
    db_handler = get_db_handler(uri, db_name, collection_name)
    run_async(db_handler.save_merged_cells, merged_cells)
    return jsonify({"status": "success"}), 200

@app.route('/get_merged_cells', methods=['POST'])
def get_merged_cells_route():
    uri = request.json.get('uri')
    db_name = request.json.get('db_name')
    collection_name = request.json.get('collection_name')
    db_handler = get_db_handler(uri, db_name, collection_name)
    result = run_async(db_handler.get_merged_cells)
    return jsonify({"status": "success", "data": result}), 200

@app.route('/append_table', methods=['POST'])
def append_table_route():
    data = request.json.get('data')
    uri = request.json.get('uri')
    db_name = request.json.get('db_name')
    collection_name = request.json.get('collection_name')
    db_handler = get_db_handler(uri, db_name, collection_name)
    run_async(db_handler.append_table, data)
    return jsonify({"status": "success"}), 200

if __name__ == '__main__':
    app.run(debug=True, port=5002)
