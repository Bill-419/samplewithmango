from flask import Flask, request, jsonify
from pymongo import MongoClient as PymongoClient
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
        self.client = PymongoClient(uri)
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

    def save_merged_cells(self, merged_cells):
        start_time = time.time()
        with lock.gen_wlock():
            step_start_time = time.time()
            self.collection.update_one({"type": "merged_cells"}, {"$set": {"merged_cells": merged_cells}}, upsert=True)
            step_end_time = time.time()
            print(f"Update one took {step_end_time - step_start_time:.4f} seconds")
        end_time = time.time()
        print(f"save_merged_cells execution time: {end_time - start_time:.4f} seconds")

    def save_all(self, data, merged_cells):
        start_time = time.time()
        with lock.gen_wlock():
            # Save table data
            step_start_time = time.time()
            self.collection.delete_many({})
            step_end_time = time.time()
            print(f"Delete many took {step_end_time - step_start_time:.4f} seconds")

            step_start_time = time.time()
            self.collection.insert_many(data)
            step_end_time = time.time()
            print(f"Insert many took {step_end_time - step_start_time:.4f} seconds")

            # Save merged cells
            step_start_time = time.time()
            self.collection.update_one({"type": "merged_cells"}, {"$set": {"merged_cells": merged_cells}}, upsert=True)
            step_end_time = time.time()
            print(f"Update one took {step_end_time - step_start_time:.4f} seconds")
        end_time = time.time()
        print(f"save_all execution time: {end_time - start_time:.4f} seconds")

    def get_all(self):
        start_time = time.time()
        with lock.gen_rlock():
            table_data = list(self.collection.find({"type": {"$ne": "merged_cells"}}))
            # Convert ObjectId to string
            for row in table_data:
                row['_id'] = str(row['_id'])
            merged_cells_data = self.collection.find_one({"type": "merged_cells"})
            merged_cells = merged_cells_data.get("merged_cells", []) if merged_cells_data else []
        end_time = time.time()
        print(f"get_all execution time: {end_time - start_time:.4f} seconds")
        return {"table_data": table_data, "merged_cells": merged_cells}

    def append_table(self, data):
        start_time = time.time()
        with lock.gen_wlock():
            self.collection.insert_many(data)
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

@app.route('/save_all', methods=['POST'])
def save_all_route():
    try:
        data = request.json.get('data')
        merged_cells = request.json.get('merged_cells')
        uri = request.json.get('uri')
        db_name = request.json.get('db_name')
        collection_name = request.json.get('collection_name')
        db_handler = get_db_handler(uri, db_name, collection_name)
        run_async(db_handler.save_all, data, merged_cells)
        return jsonify({"status": "success"}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/get_all', methods=['POST'])
def get_all_route():
    try:
        uri = request.json.get('uri')
        db_name = request.json.get('db_name')
        collection_name = request.json.get('collection_name')
        db_handler = get_db_handler(uri, db_name, collection_name)
        result = run_async(db_handler.get_all)
        return jsonify({"status": "success", "data": result}), 200
    except Exception as e:
        print(f"Error in get_all_route: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/save_table', methods=['POST'])
def save_table_route():
    try:
        data = request.json.get('data')
        uri = request.json.get('uri')
        db_name = request.json.get('db_name')
        collection_name = request.json.get('collection_name')
        db_handler = get_db_handler(uri, db_name, collection_name)
        run_async(db_handler.save_table, data)
        return jsonify({"status": "success"}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/get_table', methods=['POST'])
def get_table_route():
    try:
        uri = request.json.get('uri')
        db_name = request.json.get('db_name')
        collection_name = request.json.get('collection_name')
        db_handler = get_db_handler(uri, db_name, collection_name)
        result = run_async(db_handler.get_table)
        return jsonify({"status": "success", "data": result}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/save_merged_cells', methods=['POST'])
def save_merged_cells_route():
    try:
        merged_cells = request.json.get('merged_cells')
        uri = request.json.get('uri')
        db_name = request.json.get('db_name')
        collection_name = request.json.get('collection_name')
        db_handler = get_db_handler(uri, db_name, collection_name)
        run_async(db_handler.save_merged_cells, merged_cells)
        return jsonify({"status": "success"}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/get_merged_cells', methods=['POST'])
def get_merged_cells_route():
    try:
        uri = request.json.get('uri')
        db_name = request.json.get('db_name')
        collection_name = request.json.get('collection_name')
        db_handler = get_db_handler(uri, db_name, collection_name)
        result = run_async(db_handler.get_merged_cells)
        return jsonify({"status": "success", "data": result}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/append_table', methods=['POST'])
def append_table_route():
    try:
        data = request.json.get('data')
        uri = request.json.get('uri')
        db_name = request.json.get('db_name')
        collection_name = request.json.get('collection_name')
        db_handler = get_db_handler(uri, db_name, collection_name)
        run_async(db_handler.append_table, data)
        return jsonify({"status": "success"}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True, port=5002)
