from fastapi import APIRouter, HTTPException
import json
import os
from tqdm import tqdm  # ✅ tqdm 추가
from Database.mongodb_connection import mongo_db
from pymongo import UpdateOne

# 🚀 FastAPI 라우터 생성
router = APIRouter(
    prefix="/community_38",
    tags=["community_38"]
)

# 📂 데이터 파일 경로
data_files = {
    "Raw_38_ver1": "Non_Finance_data/38/38_ver1.json",
    "Kind_38": "Non_Finance_data/38/KIND_38.json"
}

# 📌 업로드할 컬렉션 설정
collections = {
    "Raw_38_ver1": mongo_db.Raw_38_ver1,  # 기업 게시글 데이터
    "Kind_38": mongo_db.Kind_38  # 기업 정보 데이터
}


@router.post("/upload")
def upload_community_38_data():
    """
    📂 38 커뮤니티 데이터를 MongoDB에 업로드하는 API.
    
    - `Raw_38_ver1`: 기업 게시글 데이터 (기업명_번호_제목을 `_id`로 설정)
    - `Kind_38`: 기업 정보 데이터 (기업명을 `_id`로 설정)
    """
    response_summary = {}

    for collection_name, collection in collections.items():
        try:
            # ✅ JSON 파일 로드
            file_path = data_files[collection_name]
            if not os.path.exists(file_path):
                raise FileNotFoundError(f"❌ {collection_name}: 파일이 존재하지 않습니다: {file_path}")

            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)

            print(f"✅ {collection_name}: 파일 로드 완료! 데이터 개수: {len(data)}개")

            # ✅ 컬럼 예외 처리 (MongoDB `_id` 설정)
            for record in tqdm(data, desc=f"🔍 _id 설정 중..."):
                if collection_name == "Raw_38_ver1":
                    기업명 = record.get("기업명", "").strip()
                    번호 = record.get("번호", "").strip()
                    제목 = record.get("제목", "").strip()
                    record["_id"] = f"{기업명}_{번호}_{제목}"  # ✅ `_id`는 기업명_번호_제목

                elif collection_name == "Kind_38":
                    기업명 = record.get("기업명", "").strip()
                    record["_id"] = 기업명  # ✅ `_id`는 기업명

            # ✅ 기존 데이터와 비교하여 중복 개수 확인
            total_records = len(data)
            duplicate_count = 0
            inserted_count = 0

            bulk_operations = []
            for record in tqdm(data, desc=f"🔄 중복 검사 & 업데이트 중..."):
                record_id = record["_id"]  # `_id` 값 분리
                update_data = {key: value for key, value in record.items() if key != "_id"}  # `_id` 제거

                existing_doc = collection.find_one({"_id": record_id})

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

            # ✅ MongoDB에 일괄 저장 (Bulk Write 사용)
            if bulk_operations:
                print(f"✅ MongoDB 저장 시작 ({len(bulk_operations)}개 문서)")
                for _ in tqdm(range(len(bulk_operations)), desc=f"📦 MongoDB 저장 중..."):
                    pass  # tqdm을 위한 루프, 실제 `bulk_write()` 실행은 아래에서 수행
                collection.bulk_write(bulk_operations)
                print("✅ MongoDB 저장 완료!")

            response_summary[collection_name] = {
                "total_records": total_records,
                "inserted": inserted_count,
                "duplicates": duplicate_count
            }

            print(f"✅ {collection_name} 데이터 저장 완료! (총: {total_records}개, 새 데이터: {inserted_count}개, 중복: {duplicate_count}개)")

        except Exception as e:
            raise HTTPException(status_code=500, detail=f"❌ 38 커뮤니티 데이터 업로드 중 오류 발생: {repr(e)}")

    return {
        "message": "✅ 38 커뮤니티 데이터 업로드 완료!",
        "summary": response_summary
    }