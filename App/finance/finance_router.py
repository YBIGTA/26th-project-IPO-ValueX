from fastapi import APIRouter, HTTPException
import os
import json
from Database.mongodb_connection import mongo_db

# 🚀 FastAPI 라우터 생성
router = APIRouter(
    prefix="/finance",  # API 경로 프리픽스 설정
    tags=["finance"]  # 태그 설정
)

# 📌 업로드할 금융 데이터 컬렉션 설정
collections = {
    "IPOSTOCK": mongo_db.IPOSTOCK,  # 공모주 데이터
    "Finance_by_month": mongo_db.Finance_by_month,  # 월별 금융 데이터
    "Finance_by_date": mongo_db.Finance_by_date  # 일별 금융 데이터
}

@router.post("/upload")
def upload_finance_data():
    """
    📊 로컬의 금융 데이터 JSON 파일을 읽어 MongoDB에 저장하는 API.

    - `IPOSTOCK_data.json` → `IPOSTOCK` 컬렉션
    - `Finance_by_month.json` → `Finance_by_month` 컬렉션
    - `Finance_by_date.json` → `Finance_by_date` 컬렉션
    """
    data_dir = os.path.join(os.getcwd(), "Finance_data")  # 📂 JSON 파일 위치
    files = {
        "IPOSTOCK": os.path.join(data_dir, "IPOSTOCK_data.json"),
        "Finance_by_month": os.path.join(data_dir, "Finance_by_month.json"),
        "Finance_by_date": os.path.join(data_dir, "Finance_by_date.json")
    }

    uploaded_files = {}  # ✅ 업로드된 파일 개수 저장

    for key, file_path in files.items():
        if not os.path.exists(file_path):
            print(f"⚠️ 파일 없음: {file_path}")
            continue
        
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)  # JSON 파일 로드

            if isinstance(data, dict):  # JSON이 dict 형태일 경우 list로 변환
                data = [data]
            
            collections[key].insert_many(data)  # ✅ MongoDB에 업로드
            uploaded_files[key] = len(data)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"⚠️ {file_path} 업로드 실패: {e}")

    return {"message": "✅ 금융 데이터 업로드 완료", "uploaded_files": uploaded_files}