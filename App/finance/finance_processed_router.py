from fastapi import APIRouter, HTTPException
import pandas as pd
from Database.mongodb_connection import mongo_db
from Preprocessor_Fdata.Preprocess_merge_date import run_merge_date_data
from Preprocessor_Fdata.Preprocess_finanace_final import run_merge_final_data

# 🚀 FastAPI 라우터 생성
router = APIRouter(
    prefix="/finance/processed",  # API 경로 프리픽스 설정
    tags=["finance_processed"]  # 태그 설정
)

@router.post("/process")
def process_and_store_finance_data():
    """
    📊 금융 데이터를 통합 처리 후 MongoDB에 저장하는 API.
    
    - `IPOSTOCK`, `Finance_by_month`, `Finance_by_date` 3개 컬렉션에서 데이터를 불러와 가공
    - `finance_processed` 컬렉션에 저장
    """
    # 🛢️ 기존 금융 데이터 컬렉션 불러오기
    ipostock_data = list(mongo_db.IPOSTOCK.find())
    finance_by_month = list(mongo_db.Finance_by_month.find())
    finance_by_date = list(mongo_db.Finance_by_date.find())

    # ⚠️ 데이터가 없는 경우 예외 처리
    if not ipostock_data or not finance_by_month or not finance_by_date:
        raise HTTPException(status_code=404, detail="⚠️ 하나 이상의 금융 데이터가 존재하지 않음")

    # 🏦 데이터 가공 (Processing 함수 적용)
    merged_date_data = run_merge_date_data(finance_by_date,finance_by_month)
    # ✅ DataFrame 변환 후 MongoDB 저장
    finance_summary_collection = mongo_db.Finance_processed_date
    processed_df = pd.DataFrame(merged_date_data)  # 🔄 DataFrame 변환
    finance_summary_collection.insert_many(processed_df.to_dict("records"))  # 💾 MongoDB 저장

    finance_by_date_month = list(mongo_db.Finance_processed_date.find())

    # 🏦 데이터 가공 (Processing 함수 적용)
    final_finance_data = run_merge_final_data(finance_by_date_month,ipostock_data)
    # ✅ DataFrame 변환 후 MongoDB 저장
    finance_summary_collection = mongo_db.finance_processed
    processed_df_final = pd.DataFrame(merged_date_data)  # 🔄 DataFrame 변환
    finance_summary_collection.insert_many(processed_df_final.to_dict("records"))  # 💾 MongoDB 저장

    return {"message": "✅ 금융 데이터 통합 처리 완료", "processed_records": len(processed_df)}