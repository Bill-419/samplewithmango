import requests
import time
from concurrent.futures import ThreadPoolExecutor

class MongoClient:
    def __init__(self, server_url, uri, db_name, collection_name):
        self.server_url = server_url
        self.uri = uri
        self.db_name = db_name
        self.collection_name = collection_name
        self.executor = ThreadPoolExecutor(max_workers=4)  # 根据需要调整线程池大小

    def _async_request(self, method, endpoint, payload):
        url = f"{self.server_url}/{endpoint}"
        try:
            response = requests.request(method, url, json=payload)
            response.raise_for_status()  # 检查HTTP响应状态码
            try:
                return response.json()
            except ValueError as e:
                print(f"Error decoding JSON response: {e}")
                print(f"Response content: {response.text}")
                return {"error": "Invalid JSON response"}
        except requests.RequestException as e:
            print(f"HTTP request failed: {e}")
            return {"error": str(e)}

    def save_all(self, data, merged_cells):
        start_time = time.time()
        payload = {
            "uri": self.uri,
            "db_name": self.db_name,
            "collection_name": self.collection_name,
            "data": data,
            "merged_cells": merged_cells
        }
        future = self.executor.submit(self._async_request, "POST", "save_all", payload)
        result = future.result()
        end_time = time.time()
        print(f"save_all execution time: {end_time - start_time:.4f} seconds")
        return result

    def get_all(self):
        start_time = time.time()
        payload = {
            "uri": self.uri,
            "db_name": self.db_name,
            "collection_name": self.collection_name,
        }
        future = self.executor.submit(self._async_request, "POST", "get_all", payload)
        result = future.result()
        end_time = time.time()
        print(f"get_all execution time: {end_time - start_time:.4f} seconds")
        return result

    def save_table(self, data):
        start_time = time.time()
        payload = {
            "uri": self.uri,
            "db_name": self.db_name,
            "collection_name": self.collection_name,
            "data": data
        }
        future = self.executor.submit(self._async_request, "POST", "save_table", payload)
        result = future.result()
        end_time = time.time()
        print(f"save_table execution time: {end_time - start_time:.4f} seconds")
        return result

    def get_table(self):
        start_time = time.time()
        payload = {
            "uri": self.uri,
            "db_name": self.db_name,
            "collection_name": self.collection_name,
        }
        future = self.executor.submit(self._async_request, "POST", "get_table", payload)
        result = future.result()
        end_time = time.time()
        print(f"get_table execution time: {end_time - start_time:.4f} seconds")
        return result

    def save_merged_cells(self, merged_cells):
        start_time = time.time()
        payload = {
            "uri": self.uri,
            "db_name": self.db_name,
            "collection_name": self.collection_name,
            "merged_cells": merged_cells
        }
        future = self.executor.submit(self._async_request, "POST", "save_merged_cells", payload)
        result = future.result()
        end_time = time.time()
        print(f"save_merged_cells execution time: {end_time - start_time:.4f} seconds")
        return result

    def get_merged_cells(self):
        start_time = time.time()
        payload = {
            "uri": self.uri,
            "db_name": self.db_name,
            "collection_name": self.collection_name,
        }
        future = self.executor.submit(self._async_request, "POST", "get_merged_cells", payload)
        result = future.result()
        end_time = time.time()
        print(f"get_merged_cells execution time: {end_time - start_time:.4f} seconds")
        return result

    def append_table(self, data):
        start_time = time.time()
        payload = {
            "uri": self.uri,
            "db_name": self.db_name,
            "collection_name": self.collection_name,
            "data": data
        }
        future = self.executor.submit(self._async_request, "POST", "append_table", payload)
        result = future.result()
        end_time = time.time()
        print(f"append_table execution time: {end_time - start_time:.4f} seconds")
        return result
