from fastapi import APIRouter, HTTPException
from pymongo import MongoClient
from dotenv import load_dotenv
import os

load_dotenv()

# ✅ MongoDB 연결
mongo_uri = os.getenv("MONGODB_URI")
mongo_client = MongoClient(mongo_uri, tls=True, tlsAllowInvalidCertificates=True)
mongo_db = mongo_client["Project_IPO_ValueX"]
news_collection = mongo_db["summarized_news"]

# ✅ API 엔드포인트 변경
router = APIRouter(prefix="/api/news_summary", tags=["news_summary"])

@router.get("")
async def get_news_list():
    """ 뉴스 요약 리스트 조회 """
    news_list = list(news_collection.find({}, {"_id": 1, "link": 1, "날짜": 1, "사진": 1, "요약내용": 1, "카테고리점수": 1, "태그": 1}))

    if not news_list:
        return {"status": "error", "message": "데이터가 없습니다."}

    return {"status": "success", "data": news_list}