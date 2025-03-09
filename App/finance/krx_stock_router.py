# import pandas as pd
# from datetime import datetime, timedelta
# from pykrx import stock
# from fastapi import APIRouter

# # ✅ `prefix` 및 `tags` 추가
# router = APIRouter(prefix="/api/krx", tags=["krx"])

# # ✅ 날짜 설정 (코스피 & 코스닥: 30일, 나머지 지수: 7일)
# end_date = datetime.now().strftime("%Y%m%d")
# start_date_general = (datetime.now() - timedelta(days=7)).strftime("%Y%m%d")
# start_date_kospi_kosdaq = (datetime.now() - timedelta(days=30)).strftime("%Y%m%d")

# # ✅ 지수명 → KRX 코드 매핑
# INDEX_CODES = {
#     "kospi": "1001",
#     "kosdaq": "2001",
#     "semiconductor": "1013",
#     "medical": "1014",
#     "chemical": "1008",
#     "machinery": "1012",
#     "service": "1026",
#     "food": "1005",
#     "banking": "1021",
#     "infra": "1045",  # ✅ infra 지수 코드 확인 필요
# }

# def get_index_data_by_code(sector_code: str, days: int):
#     """ 특정 섹터 코드의 KRX 주가지수 데이터 가져오기 """
#     start_date = (datetime.now() - timedelta(days=days)).strftime("%Y%m%d")
    
#     print(f"📊 {sector_code} 지수 데이터 크롤링 중... ({start_date} ~ {end_date})")

#     try:
#         sector_data = stock.get_index_ohlcv_by_date(start_date, end_date, sector_code)[['시가', '종가', '거래량']]
#         sector_data.reset_index(inplace=True)  # 날짜 컬럼 변환
#         sector_data['날짜'] = sector_data['날짜'].dt.strftime("%Y-%m-%d")  # 날짜 변환

#         if sector_data.empty:
#             print(f"❌ {sector_code} 지수 크롤링 실패: 데이터 없음")
#             return []

#         print(f"✅ {sector_code} 지수 크롤링 완료 ({len(sector_data)}개 데이터)")
#         return sector_data.to_dict(orient="records")  # JSON 변환

#     except Exception as e:
#         print(f"🚨 {sector_code} 지수 크롤링 중 오류 발생: {str(e)}")
#         return []

# @router.get("/{index_name}")
# async def get_index_data(index_name: str):
#     """
#     📊 특정 KRX 주가지수 데이터를 가져오는 API  
#     - `/api/krx/kospi` → KOSPI 지수 데이터 (30일치)  
#     - `/api/krx/kosdaq` → KOSDAQ 지수 데이터 (30일치)  
#     - `/api/krx/semiconductor` → 반도체 지수 데이터 (7일치)  
#     - `/api/krx/medical` → 의료·의약 지수 데이터 (7일치)  
#     - `/api/krx/chemical` → 화학 지수 데이터 (7일치)  
#     - `/api/krx/machinery` → 기계 지수 데이터 (7일치)  
#     - `/api/krx/service` → 서비스 지수 데이터 (7일치)  
#     - `/api/krx/food` → 식료·기호품 지수 데이터 (7일치)  
#     - `/api/krx/banking` → 은행·보험·증권 데이터 (7일치)  
#     - `/api/krx/infra` → 인프라 지수 데이터 (7일치)  
#     """
#     print(f"🔹 요청된 지수명: {index_name}")
    
#     sector_code = INDEX_CODES.get(index_name.lower())
#     if not sector_code:
#         return {"status": "error", "message": "유효하지 않은 지수명입니다."}
    
#     # ✅ 코스피 & 코스닥은 30일치, 나머지는 7일치 데이터 요청
#     days = 30 if index_name.lower() in ["kospi", "kosdaq"] else 30
    
#     data = get_index_data_by_code(sector_code, days)
#     return {"status": "success", "data": data}


from fastapi import APIRouter, HTTPException
import numpy as np
import traceback
from bson import json_util
from Database.mongodb_connection import mongo_db  # ✅ connection.py에서 DB 연결 가져옴

# ✅ FastAPI 라우터 설정
router = APIRouter(prefix="/api/krx", tags=["krx"])

# ✅ 지수명 → KRX 컬럼 매핑
INDEX_CODES = {
    "kospi": "1001",
    "kosdaq": "2001",
    "semiconductor": "1013",
    "medical": "1014",
    "chemical": "1008",
    "machinery": "1012",
    "service": "1026",
    "food": "1005",
    "banking": "1021",
    "infra": "1045",
}

def clean_data(data):
    """MongoDB에서 가져온 데이터에서 NaN 값을 None으로 변환"""
    for doc in data:
        for key, value in doc.items():
            if isinstance(value, float) and (np.isnan(value) or value is None):
                doc[key] = None  # JSON 변환을 위해 NaN을 None으로 변경
    return data

@router.get("/{index_name}")
async def get_index_data(index_name: str):
    """
    📊 특정 KRX 주가지수 데이터를 MongoDB에서 조회하는 API  
    - `/api/krx/kospi` → KOSPI 지수 데이터 (30일치)  
    - `/api/krx/semiconductor` → 반도체 지수 데이터 (30일치)  
    - `/api/krx/banking` → 은행 지수 데이터 (30일치)  
    """
    try:
        print(f"🔹 요청된 지수명: {index_name}")

        sector_column = f"KOSPI_new{INDEX_CODES.get(index_name.lower())}"
        print(f"📌 요청된 지수명: {index_name} → 컬럼명: {sector_column}")

        collection = mongo_db["krx_data"]
        if collection is None:
            raise HTTPException(status_code=500, detail="MongoDB 연결 실패")

        # ✅ MongoDB에서 최근 30일 데이터 조회
                # ✅ MongoDB에서 2025-01-24 ~ 2025-02-24 데이터 조회
        filter_query = {
            "date": {"$gte": "2025-01-24", "$lte": "2025-02-24"}
        }  # 특정 날짜 범위 설정

        projection = {"_id": 0, "date": 1, sector_column: 1}  # 특정 컬럼만 선택
        data = list(collection.find(filter_query, projection))

        if not data:
            raise HTTPException(status_code=404, detail="데이터 없음")

        # ✅ NaN 값 처리
        cleaned_data = clean_data(data)

        return {"status": "success", "data": cleaned_data}

    except Exception as e:
        print(f"🚨 오류 발생: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=str(e))