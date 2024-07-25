from flask import Flask, request, jsonify
from pymongo import MongoClient
import json
from concurrent.futures import ThreadPoolExecutor
import threading

app = Flask(__name__)
executor = ThreadPoolExecutor(max_workers=4)  # 你可以根据需求调整线程池大小
lock = threading.Lock()

# 全局变量来存储 MongoDBHandler 实例
db_handlers = {}

class MongoDBHandler:
    def __init__(self, uri, db_name, collection_name):
        self.client = MongoClient(uri)
        self.db = self.client[db_name]
        self.collection = self.db[collection_name]

    def save_table(self, data):
        with lock:
            self.collection.delete_many({})
            self.collection.insert_many(data)

    def get_table(self):
        with lock:
            return list(self.collection.find({}, {"_id": 0}))

    def append_table(self, data):
        with lock:
            self.collection.insert_many(data)

def run_async(func, *args):
    future = executor.submit(func, *args)
    return future.result()

def get_db_handler(uri, db_name, collection_name):
    key = (uri, db_name, collection_name)
    with lock:
        if key not in db_handlers:
            db_handlers[key] = MongoDBHandler(uri, db_name, collection_name)
        return db_handlers[key]

@app.route('/save_table', methods=['POST'])
def save_table():
    data = request.json.get('data')
    uri = request.json.get('uri')
    db_name = request.json.get('db_name')
    collection_name = request.json.get('collection_name')
    db_handler = get_db_handler(uri, db_name, collection_name)
    run_async(db_handler.save_table, data)
    return jsonify({"status": "success"}), 200

@app.route('/get_table', methods=['POST'])
def get_table():
    uri = request.json.get('uri')
    db_name = request.json.get('db_name')
    collection_name = request.json.get('collection_name')
    db_handler = get_db_handler(uri, db_name, collection_name)
    result = run_async(db_handler.get_table)
    return jsonify({"status": "success", "data": result}), 200

@app.route('/append_table', methods=['POST'])
def append_table():
    data = request.json.get('data')
    uri = request.json.get('uri')
    db_name = request.json.get('db_name')
    collection_name = request.json.get('collection_name')
    db_handler = get_db_handler(uri, db_name, collection_name)
    run_async(db_handler.append_table, data)
    return jsonify({"status": "success"}), 200

if __name__ == '__main__':
    app.run(debug=True, port=5009)
