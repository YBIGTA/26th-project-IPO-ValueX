from pymongo import MongoClient
from dotenv import load_dotenv
import os

load_dotenv()

mongo_uri = os.getenv("MONGODB_URI")
mongo_client = MongoClient(mongo_uri)

mongo_db = mongo_client["db"]

print(mongo_db.list_collection_names())