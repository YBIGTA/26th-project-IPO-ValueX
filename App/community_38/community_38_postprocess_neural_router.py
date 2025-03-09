from fastapi import APIRouter, HTTPException
from Database.mongodb_connection import mongo_db
from Sentiment_modeling.Sentiment_38_and_postprocess import run_38_sentiment_ver2
from pymongo import UpdateOne

# 🚀 FastAPI 라우터 생성
router = APIRouter(
    prefix="/community_38",
    tags=["community_38"]
)

@router.post("/postprocess_neural")
def postprocess_neural_community_38():
    """
    🧠 38 커뮤니티 데이터를 뉴럴팩터 모델링용으로 가공하는 API.
    
    - `Processed_38` 데이터를 가져와 감성 분석 모델에 적합한 형태로 변환.
    - 변환된 데이터를 `Sentiment_38` 컬렉션에 저장.
    """
    try:
        # ✅ 원본 데이터 가져오기
        processed_data = list(mongo_db.Processed_38.find({}))

        if not processed_data:
            raise HTTPException(status_code=500, detail="❌ `Processed_38` 데이터가 없습니다. 먼저 `/community_38/processed` 실행하세요.")

        # ✅ 뉴럴팩터 모델링용 데이터 전처리 실행
        neural_data = run_38_sentiment_ver2(processed_data)

        # ✅ 컬럼 예외 처리 (`_id` 설정)
        for record in neural_data:
            record["_id"] = record.get("기업명", "").strip()  # `_id`는 기업명

        # ✅ 기존 데이터와 비교하여 중복 개수 확인
        total_records = len(neural_data)
        duplicate_count = 0
        inserted_count = 0

        bulk_operations = []
        for record in neural_data:
            record_id = record["_id"]
            update_data = {key: value for key, value in record.items() if key != "_id"}

            existing_doc = mongo_db.Sentiment_38.find_one({"_id": record_id})

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

        # ✅ MongoDB에 저장 (Bulk Write 사용)
        if bulk_operations:
            mongo_db.Sentiment_38.bulk_write(bulk_operations)

        print(f"✅ Sentiment_38 저장 완료! (총: {total_records}개, 새 데이터: {inserted_count}개, 중복: {duplicate_count}개)")

        return {
            "message": "✅ 38 커뮤니티 데이터 뉴럴팩터 모델링용 가공 완료!",
            "summary": {
                "total_records": total_records,
                "inserted": inserted_count,
                "duplicates": duplicate_count
            }
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"❌ 38 커뮤니티 데이터 뉴럴팩터 모델링 가공 중 오류 발생: {repr(e)}")