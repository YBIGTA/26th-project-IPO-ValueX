from fastapi import APIRouter, HTTPException
import json
import os
from tqdm import tqdm
from Database.mongodb_connection import mongo_db
from Preprocessor_NFdata.Preprocess_38 import run_preprocess_38
from pymongo import UpdateOne
from typing import Literal

# 🚀 FastAPI 라우터 생성
router = APIRouter(
    prefix="/community_38",
    tags=["community_38"]
)

# 📂 로컬 데이터 파일 경로
local_data_files = {
    "Raw_38_ver1": "Non_Finance_data/38/38_ver1.json",
    "Kind_38": "Non_Finance_data/38/KIND_38.json"
}

@router.post("/processed")
def process_community_38_data(mode: Literal["db", "local"] = "db"):
    """
    🏛 38 커뮤니티 데이터 전처리 API.

    - **DB 모드** (`mode="db"`): MongoDB의 `Raw_38_ver1`과 `Kind_38` 데이터를 가져와 `Processed_38`을 생성.
    - **로컬 모드** (`mode="local"`): 로컬 JSON 파일에서 데이터를 가져와 `Processed_38`을 생성.

    - MongoDB의 `Processed_38` 컬렉션에 저장.
    """
    try:
        if mode == "db":
            # ✅ MongoDB에서 JSON 데이터 가져오기
            print("📡 MongoDB에서 데이터 로드 중...")
            raw_data = list(tqdm(mongo_db.Raw_38_ver1.find({}, {"_id": 0}), desc="🔄 Loading Raw_38_ver1"))
            kind_data = list(tqdm(mongo_db.Kind_38.find({}, {"_id": 0}), desc="🔄 Loading Kind_38"))
            data_source = "📡 MongoDB에서 데이터 로드"

        elif mode == "local":
            # ✅ 로컬 JSON 파일에서 데이터 가져오기
            if not os.path.exists(local_data_files["Raw_38_ver1"]) or not os.path.exists(local_data_files["Kind_38"]):
                raise HTTPException(status_code=500, detail="❌ 로컬 데이터 파일이 존재하지 않습니다.")

            print("💾 로컬 JSON 파일에서 데이터 로드 중...")
            with open(local_data_files["Raw_38_ver1"], "r", encoding="utf-8") as f:
                raw_data = json.load(f)

            with open(local_data_files["Kind_38"], "r", encoding="utf-8") as f:
                kind_data = json.load(f)

            data_source = "💾 로컬 JSON에서 데이터 로드"

        else:
            raise HTTPException(status_code=400, detail="❌ 지원되지 않는 모드입니다. 'db' 또는 'local'을 선택하세요.")

        if not raw_data or not kind_data:
            raise HTTPException(status_code=500, detail=f"❌ 원본 데이터가 부족합니다. (모드: {mode})")

        print(f"✅ 데이터 소스: {data_source}")

        # ✅ 데이터 전처리 실행 (진행 표시)
        print("⚙️ 데이터 전처리 중...")
        processed_data = run_preprocess_38(raw_data, kind_data)
        processed_data = list(tqdm(processed_data, desc="🚀 Processing Data"))

       # ✅ 컬럼 예외 처리 (`_id = 기업명_텍스트` 설정)
        for record in processed_data:
            기업명 = record.get("기업명", "").strip()
            텍스트 = record.get("텍스트", "").strip()

            # `_id`는 `기업명_텍스트` 조합 (띄어쓰기 제거)
            record["_id"] = f"{기업명}_{텍스트.replace(' ', '')}"

        # ✅ 기존 데이터와 비교하여 중복 개수 확인
        total_records = len(processed_data)
        duplicate_count = 0
        inserted_count = 0

        bulk_operations = []
        for record in tqdm(processed_data, desc="📦 Checking for Duplicates"):
            record_id = record["_id"]
            update_data = {key: value for key, value in record.items() if key != "_id"}

            existing_doc = mongo_db.Processed_38.find_one({"_id": record_id})

            if existing_doc:
                duplicate_count += 1
            else:
                inserted_count += 1
                bulk_operations.append(
                    UpdateOne(
                        {"_id": record_id},  # 필터
                        {"$set": update_data},  # 업데이트
                        upsert=True  # ✅ 존재하면 업데이트, 없으면 삽입
                    )
                )

        # ✅ MongoDB에 저장 (Bulk Write 사용, 진행 표시)
        if bulk_operations:
            print(f"📦 MongoDB 저장 중... ({len(bulk_operations)}개 문서)")
            # mongo_db.Processed_38.bulk_write(tqdm(bulk_operations, desc="💾 Saving to MongoDB"))

            # ✅ 수정된 코드 (tqdm을 제외하고 순수 리스트만 전달)
            for operation in tqdm(bulk_operations, desc="💾 Saving to MongoDB"):
                mongo_db.Processed_38.bulk_write([operation])

        print(f"✅ Processed_38 저장 완료! (총: {total_records}개, 새 데이터: {inserted_count}개, 중복: {duplicate_count}개)")



        return {
            "message": "✅ 38 커뮤니티 데이터 전처리 및 저장 완료!",
            "mode": mode,
            "summary": {
                "total_records": total_records,
                "inserted": inserted_count,
                "duplicates": duplicate_count
            }
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"❌ 38 커뮤니티 데이터 전처리 중 오류 발생: {repr(e)}")