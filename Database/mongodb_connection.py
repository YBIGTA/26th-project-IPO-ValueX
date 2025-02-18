from pymongo import MongoClient
from dotenv import load_dotenv
import os

load_dotenv()

mongo_uri = os.getenv("MONGODB_URI")
mongo_client = MongoClient(mongo_uri)

# 여기 고쳐야 함. 일단 테스트를 위해서 movie_review_db 를 가져온 것
mongo_db = mongo_client["movie_review_db"]

print(mongo_db.list_collection_names())