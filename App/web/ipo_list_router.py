from fastapi import APIRouter
from datetime import datetime
from pymongo import MongoClient
from dotenv import load_dotenv
import os

router = APIRouter(prefix="/api/ipo", tags=["IPO"])

# ✅ 환경 변수 로드
load_dotenv()

# ✅ MongoDB 연결
mongo_uri = os.getenv("MONGODB_URI")
mongo_client = MongoClient(mongo_uri, tls=True, tlsAllowInvalidCertificates=True)
mongo_db = mongo_client["Project_IPO_ValueX"]
collection = mongo_db["company_by_date"]

today = "2025-02-10";#'datetime.now().strftime("%Y-%m-%d")

@router.get("/list")
async def get_upcoming_ipos():
    """ 현재 날짜 이후 상장하는 기업 목록 반환 """
    # ✅ 전체 데이터 개수 조회
    total_count = collection.count_documents({})

    results = collection.find({"상장일": {"$gte": today}}, {"_id": 0, "기업명": 1, "상장일": 1, "이진분류":1, "회귀": 1})
    company_list = list(results)
    return {
        "status": "success",
        "total_companies": total_count,   # ✅ 전체 기업 개수
        "upcoming_companies": len(company_list),  # ✅ 현재 날짜 이후 기업 개수
        "data": company_list
    }

@router.get("/{company_name}")
async def get_ipo_details(company_name: str):
    """ 선택한 기업의 상장일 반환 """
    result = collection.find_one({"기업명": company_name}, {"_id": 0, "상장일": 1, "이진분류":1, "회귀":1})
    if result:
        return {"status": "success", "data": result}
    return {"status": "error", "message": "기업을 찾을 수 없습니다."}