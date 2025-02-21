from fastapi import APIRouter, HTTPException
import pandas as pd
from Database.mongodb_connection import mongo_db
from Preprocessor_Fdata.Preprocess_merge_date import run_merge_date_data
from Preprocessor_Fdata.Preprocess_finanace_final import run_merge_final_data
from pymongo import UpdateOne
from tqdm import tqdm

# 🚀 FastAPI 라우터 생성
router = APIRouter(
    prefix="/finance/merged",
    tags=["finance_merged"]
)

@router.post("/process_date")
def process_and_store_finance_data():
    """
    📊 `Finance_by_date` & `Finance_by_month` → `Finance_processed_date` 저장
    """
    # 🛢️ 기존 금융 데이터 불러오기
    finance_by_date = pd.DataFrame(list(mongo_db.Finance_by_date.find()))
    finance_by_month = pd.DataFrame(list(mongo_db.Finance_by_month.find()))

    # ⚠️ 데이터 유효성 검사
    if finance_by_date.empty or finance_by_month.empty:
        raise HTTPException(status_code=404, detail="❌ 일별 & 월별 금융 데이터 부족")

    # ✅ `Finance_processed_date` 생성
    df_merge = run_merge_date_data(finance_by_date, finance_by_month)

    # ✅ `_id`를 `date` 컬럼으로 설정
    if "date" not in df_merge.columns:
        raise HTTPException(status_code=500, detail="❌ `date` 컬럼이 누락되었습니다.")
    df_merge["_id"] = df_merge["date"]

    finance_processed_collection = mongo_db.Finance_processed_date

    inserted_count = 0
    updated_count = 0

    print("🚀 `Finance_processed_date` 업로드 중...")
    for record in tqdm(df_merge.to_dict("records"), desc="Uploading Finance_processed_date"):
        record_id = record["_id"]
        update_data = {key: value for key, value in record.items() if key != "_id"}

        update_result = finance_processed_collection.update_one(
            {"_id": record_id}, {"$set": update_data}, upsert=True
        )

        if update_result.upserted_id:
            inserted_count += 1
        elif update_result.modified_count > 0:
            updated_count += 1

    print(f"✅ `Finance_processed_date` 저장 완료! (총: {len(df_merge)}개, 새 데이터: {inserted_count}개, 갱신: {updated_count}개)")

    return {
        "message": "✅ 금융 데이터 병합 완료!",
        "processed_records": {
            "Finance_processed_date": {
                "total": len(df_merge),
                "inserted": inserted_count,
                "updated": updated_count
            }
        }
    }

@router.post("/process_final")
def process_and_store_final_data():
    """
    📊 `Finance_processed_date` & `IPOSTOCK` → `Finance_final_processed` 저장
    """
    # 🛢️ 기존 금융 데이터 불러오기
    finance_processed_date = pd.DataFrame(list(mongo_db.Finance_processed_date.find()))
    ipostock_data = pd.DataFrame(list(mongo_db.IPOSTOCK.find()))

    # ⚠️ 데이터 유효성 검사
    if finance_processed_date.empty:
        raise HTTPException(status_code=404, detail="❌ 병합된 금융 데이터 부족")
    if ipostock_data.empty:
        raise HTTPException(status_code=404, detail="❌ 공모주 데이터 부족")

    # ✅ `Finance_final_processed` 생성
    df_final = run_merge_final_data(finance_processed_date, ipostock_data)

    # ✅ `_id`를 `date` 컬럼으로 설정
    if "date" not in df_final.columns:
        raise HTTPException(status_code=500, detail="❌ `date` 컬럼이 누락되었습니다.")
    df_final["_id"] = df_final["date"]

    final_finance_collection = mongo_db.Finance_final_processed

    inserted_count_final = 0
    updated_count_final = 0

    print("🚀 `Finance_final_processed` 업로드 중...")
    for record in tqdm(df_final.to_dict("records"), desc="Uploading Finance_final_processed"):
        record_id = record["_id"]
        update_data = {key: value for key, value in record.items() if key != "_id"}

        update_result = final_finance_collection.update_one(
            {"_id": record_id}, {"$set": update_data}, upsert=True
        )

        if update_result.upserted_id:
            inserted_count_final += 1
        elif update_result.modified_count > 0:
            updated_count_final += 1

    print(f"✅ `Finance_final_processed` 저장 완료! (총: {len(df_final)}개, 새 데이터: {inserted_count_final}개, 갱신: {updated_count_final}개)")

    return {
        "message": "✅ 금융 데이터 최종 병합 완료!",
        "processed_records": {
            "Finance_final_processed": {
                "total": len(df_final),
                "inserted": inserted_count_final,
                "updated": updated_count_final
            }
        }
    }