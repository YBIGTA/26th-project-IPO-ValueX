from pymongo import MongoClient
from dotenv import load_dotenv
import os

load_dotenv()

mongo_uri = os.getenv("MONGODB_URI")
#TLS 옵션 추가
mongo_client = MongoClient(mongo_uri, tls=True, tlsAllowInvalidCertificates=True)

# 몰아넣은 새 db 정의
mongo_db = mongo_client["Project_IPO_ValueX"]

print(f"✅ 현재 collection 리스트 : {mongo_db.list_collection_names()}")