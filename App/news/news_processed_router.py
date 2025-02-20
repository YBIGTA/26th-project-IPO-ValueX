import json
import os
import pandas as pd
from tqdm import tqdm  # ✅ tqdm 추가
from fastapi import APIRouter, HTTPException, Query
from Database.mongodb_connection import mongo_db
from LLM_modeling.vectorize.article_summarize import NewsTokenizer

# 🚀 FastAPI 라우터 생성
router = APIRouter(
    prefix="/summary",
    tags=["summary"]
)

@router.post("/summarize/data")
def summarize_and_vectorize_news(mode: str = Query("db", description="실행 모드: 'local' 또는 'db'")):
    """
    📰 뉴스 문서를 요약하고 벡터화하여 MongoDB에 저장하는 API 엔드포인트.
    
    - **mode="local"** → 로컬 CSV 파일에서 뉴스 데이터를 불러와 요약 후 저장
    - **mode="db"** → MongoDB에서 뉴스 데이터를 불러와 요약 후 저장
    """
    summary_collection = mongo_db.summary_and_vectors  # ✅ 요약 결과 저장할 MongoDB 컬렉션
    news_tokenizer = NewsTokenizer(
        peft_model_dir="LLM_modeling/finetuning/mt5_large_peft_final",
        dataset_file="dummy",
        output_file="LLM_modeling/backup/summary_backup.json"
    )

    batch_size = 3  # 🔄 배치 크기 설정
    batch_records = []  # 저장할 데이터 리스트
    processed_count = 0  # ✅ 처리된 문서 개수
    error_count = 0  # ❌ 에러 발생 문서 개수
    duplicate_count = 0  # 🔄 중복된 데이터 개수

    def convert_oid(record):
        """MongoDB ObjectId를 문자열로 변환"""
        rec = record.copy()
        rec["_id"] = str(rec["_id"])
        return rec

    if mode == "db":
        # ✅ MongoDB에서 뉴스 데이터 가져오기
        doc_collection = mongo_db.preprocessed_news
        documents = list(doc_collection.find())  # ✅ 리스트로 변환하여 tqdm 적용 가능하게 만듦

    elif mode == "local":
        # ✅ 로컬 CSV에서 데이터 불러오기
        raw_path = os.path.join(os.getcwd(), "Non_Finance_data", "Naver_Stock")
        files = [os.path.join(raw_path, file) for file in os.listdir(raw_path) if file.endswith('.csv')]

        if not files:
            raise HTTPException(status_code=404, detail=f"No news files found in {raw_path}")

        all_data = []
        for file in files:
            df = pd.read_csv(file, encoding="utf-8-sig", on_bad_lines="skip")
            all_data.extend(df.to_dict('records'))

        documents = all_data  # ✅ MongoDB에서 읽은 데이터처럼 리스트 형태로 변환

    else:
        raise HTTPException(status_code=400, detail="잘못된 모드입니다. 'local' 또는 'db'를 선택하세요.")

    # 🔍 문서 순회하며 요약 및 벡터화 진행 (✅ tqdm 적용)
    for doc in tqdm(documents, desc="Processing news summarization", unit="doc"):
        doc_id = doc.get("Link", None)  # ✅ `_id`를 `Link` 값으로 설정
        if not doc_id:
            continue  # `_id`가 없으면 스킵

        if summary_collection.find_one({"_id": doc_id}):
            duplicate_count += 1  # ✅ 중복 개수 증가
            continue  # 이미 처리된 문서는 스킵

        try:
            article = doc.get("Body_processed", "") if mode == "db" else doc.get("Body", "")
            if not isinstance(article, str) or not article.strip():
                print(f"⚠️ Invalid or empty 'Body_processed' in document with _id {doc_id}. Skipping.")
                continue

            entity = {
                "id": str(doc_id),
                "article": article
            }
            result = news_tokenizer.summarize_and_tokenize(entity)  # 🔄 뉴스 요약 및 벡터화
            result["_id"] = doc_id  # ✅ `_id`를 `Link` 값으로 설정

            batch_records.append(result)
            processed_count += 1

            # ✅ 배치 크기에 도달하면 DB에 저장
            if len(batch_records) >= batch_size:
                for record in batch_records:
                    summary_collection.update_one(
                        {"_id": record["_id"]},
                        {"$set": record},
                        upsert=True
                    )
                with open(news_tokenizer.output_file, "a", encoding="utf-8") as f:
                    backup_records = [convert_oid(rec) for rec in batch_records]
                    json.dump(backup_records, f, ensure_ascii=False, indent=4)
                    f.write("\n")
                batch_records = []

        except Exception as e:
            print(f"❌ Error processing document with _id {doc_id} - {e}")
            error_count += 1
            continue

    # 🔄 남은 배치 데이터 처리
    if batch_records:
        for record in batch_records:
            summary_collection.update_one(
                {"_id": record["_id"]},
                {"$set": record},
                upsert=True
            )
        with open(news_tokenizer.output_file, "a", encoding="utf-8") as f:
            backup_records = [convert_oid(rec) for rec in batch_records]
            json.dump(backup_records, f, ensure_ascii=False, indent=4)
            f.write("\n")

    print(f"✅ 요약 완료: 총 {len(documents)}개 중 {processed_count}개 저장됨, {duplicate_count}개 중복으로 저장 안됨")

    return {
        "message": f"✅ Summarization and vectorization completed using mode: {mode}",
        "processed": processed_count,
        "duplicates": duplicate_count,
        "errors": error_count
    }