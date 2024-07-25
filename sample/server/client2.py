# client.py
import requests

class MongoClient:
    def __init__(self, server_url, uri, db_name, collection_name):
        self.server_url = server_url
        self.uri = uri
        self.db_name = db_name
        self.collection_name = collection_name

    def save_table(self, data):
        payload = {
            "uri": self.uri,
            "db_name": self.db_name,
            "collection_name": self.collection_name,
            "data": data
        }
        response = requests.post(f"{self.server_url}/save_table", json=payload)
        return response.json()

    def get_table(self):
        payload = {
            "uri": self.uri,
            "db_name": self.db_name,
            "collection_name": self.collection_name,
        }
        response = requests.post(f"{self.server_url}/get_table", json=payload)
        return response.json()

    def append_table(self, data):
        payload = {
            "uri": self.uri,
            "db_name": self.db_name,
            "collection_name": self.collection_name,
            "data": data
        }
        response = requests.post(f"{self.server_url}/append_table", json=payload)
        return response.json()
