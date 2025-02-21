from fastapi import APIRouter, HTTPException
import pandas as pd
import os
from Database.mongodb_connection import mongo_db
from Preprocessor_Fdata.Preprocess_daily import run_process_daily
from Preprocessor_Fdata.Preprocess_ipostock import run_process_ipostock
from Preprocessor_Fdata.Preprocess_monthly import run_process_monthly
from pymongo import UpdateOne

# 🚀 FastAPI 라우터 생성
router = APIRouter(
    prefix="/finance",
    tags=["finance"]
)

# 📌 업로드할 금융 데이터 컬렉션 설정
collections = {
    "IPOSTOCK": mongo_db.IPOSTOCK,  # 공모주 데이터 (기업명 기준 저장)
    "Finance_by_month": mongo_db.Finance_by_month,  # 월별 금융 데이터 (month_key + index 기준 저장)
    "Finance_by_date": mongo_db.Finance_by_date  # 일별 금융 데이터 (date 기준 저장)
}

# 📂 데이터 파일 경로
data_files = {
    "IPOSTOCK": "Finance_data/X_stat.csv",
    "Finance_by_month": "Finance_data/df_monthly.csv",
    "Finance_by_date": "Finance_data/df_daily.csv"
}

# 🏦 처리할 전처리 함수
preprocess_functions = {
    "IPOSTOCK": run_process_ipostock,
    "Finance_by_month": run_process_monthly,
    "Finance_by_date": run_process_daily
}


@router.post("/upload")
def upload_finance_data(load_from_json: bool = False):
    """
    📂 금융 데이터를 업로드하고 가공하여 MongoDB에 저장하는 API.
    
    - `load_from_json=True` → 로컬 JSON 데이터 처리 후 업로드
    - `load_from_json=False` → 기존 CSV 데이터 로드 후 업로드
    """
    response_summary = {}

    for collection_name, collection in collections.items():
        try:
            # ✅ JSON 데이터에서 직접 처리
            if collection_name == "Finance_by_date" and load_from_json:
                json_dir = "Finance_data/etc"
                processed_df = run_process_daily(json_dir=json_dir)
            elif collection_name == "Finance_by_month" and load_from_json:
                json_dir = "Finance_data/etc"
                processed_df = run_process_monthly(json_dir=json_dir)
            elif collection_name == "IPOSTOCK" and load_from_json:
                input_json = "Finance_data/etc/IPOSTOCK_data.json"
                processed_df = run_process_ipostock(json_file=input_json)
            else:
                # ✅ CSV 파일 로드
                file_path = data_files[collection_name]
                processed_df = pd.read_csv(file_path, encoding="utf-8-sig")

                # ✅ 데이터 가공 (전처리 함수 적용)
                processed_df = preprocess_functions[collection_name](processed_df)

            # ✅ 컬럼 예외 처리 (MongoDB `_id` 설정)
            if collection_name == "Finance_by_date":
                if "date" not in processed_df.columns:
                    raise KeyError(f"❌ {collection_name}: 'date' 컬럼이 없습니다. CSV 파일을 확인하세요.")
                processed_df["_id"] = processed_df["date"]  # ✅ 일별 데이터의 `_id`는 `date`

            elif collection_name == "Finance_by_month":
                if "month_key" not in processed_df.columns:
                    raise KeyError(f"❌ {collection_name}: 'month_key' 컬럼이 없습니다. CSV 파일을 확인하세요.")

                # ✅ `_id`를 `month_key` + index로 설정하여 중복 방지
                processed_df["_id"] = processed_df["month_key"] # ✅ 월별 데이터의 `_id`는 `month_key`

            elif collection_name == "IPOSTOCK":
                if "기업명" not in processed_df.columns:
                    raise KeyError(f"❌ {collection_name}: '기업명' 컬럼이 없습니다. CSV 파일을 확인하세요.")
                processed_df["_id"] = processed_df["기업명"]  # ✅ 공모주 데이터의 `_id`는 `기업명`

            # ✅ 기존 데이터와 비교하여 중복 개수 확인
            total_records = len(processed_df)
            duplicate_count = 0
            inserted_count = 0

            bulk_operations = []
            for record in processed_df.to_dict("records"):
                record_id = record["_id"]  # `_id` 값 분리
                update_data = {key: value for key, value in record.items() if key != "_id"}  # `_id` 제거

                existing_doc = collection.find_one({"_id": record_id})

                if existing_doc:
                    duplicate_count += 1
                    bulk_operations.append(
                        UpdateOne(
                            {"_id": record_id},  # 필터
                            {"$set": update_data},  # 업데이트
                            upsert=True  # ✅ 존재하면 업데이트, 없으면 삽입
                        )
                    )
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
                result = collection.bulk_write(bulk_operations)
                inserted_count = result.upserted_count
                duplicate_count = result.matched_count

            response_summary[collection_name] = {
                "total_records": total_records,
                "inserted": inserted_count,
                "updated": duplicate_count
            }

            print(f"✅ {collection_name} 데이터 저장 완료! (총: {total_records}개, 새 데이터: {inserted_count}개, 업데이트: {duplicate_count}개)")

        except KeyError as ke:
            raise HTTPException(status_code=500, detail=f"❌ 금융 데이터 업로드 중 오류 발생: {str(ke)}")

        except Exception as e:
            raise HTTPException(status_code=500, detail=f"❌ 금융 데이터 업로드 중 오류 발생: {repr(e)}")

    return {
        "message": "✅ 금융 데이터 업로드 및 저장 완료!",
        "summary": response_summary
    }