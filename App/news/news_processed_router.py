from fastapi import APIRouter, HTTPException, Query
from Database.mongodb_connection import mongo_db
import os
import time
import pandas as pd
import re
from tqdm import tqdm  # ✅ tqdm 추가
from Preprocessor_NFdata.Preprocess_news import run_preprocess_naver
from Preprocessor_NFdata.Preprocess_tfidf import run_semi_tfidf
# from Preprocessor_NFdata.Preprocess_tfidf_tokenized import run_tfidf
from Crawler.naver_news_crawler import run_crawler

# FastAPI 라우터 생성
router = APIRouter(
    prefix="/news",
    tags=["news"]
)

def extract_year_from_filename(filename):
    """ 📌 파일명에서 YYYY 연도 추출하는 함수 """
    match = re.search(r'(\d{4})', filename)  # 4자리 숫자 찾기
    return int(match.group(1)) if match else None  # 찾으면 int 변환 후 반환, 없으면 None

@router.post("/preprocess/news")
def preprocess_news(
    mode: str = Query("local", description="실행 모드: 'local' 또는 'crawler'"),
    years: str = Query(None, description="처리할 연도 리스트 (쉼표로 구분된 문자열)")
):
    """
    📰 네이버 주식 뉴스 데이터를 전처리하고, 결과를 MongoDB에 저장하는 API 엔드포인트.
    
    - **mode="local"** → 로컬 파일을 읽어서 전처리 후 DB에 저장
    - **mode="crawler"** → 크롤러에서 바로 데이터를 받아서 전처리 후 DB에 저장
    - **years="2022,2023"** → 특정 연도(YYYY)만 선택적으로 처리
    """
    preprocessed_news_collection = mongo_db.preprocessed_news  # 전처리된 뉴스 컬렉션

    # 📌 선택한 연도 리스트 변환 (예: "2022,2023" → [2022, 2023])
    selected_years = [int(year.strip()) for year in years.split(",")] if years else None

    if mode == "local":
        # 📌 로컬 파일 모드
        raw_path = os.path.join(os.getcwd(), "Non_Finance_data", "Naver_Stock")  # 원본 뉴스 데이터 경로
        files = [os.path.join(raw_path, file) for file in os.listdir(raw_path)]

        category_path = os.path.join(os.getcwd(), "Database", "sector_vocab")  # 산업별 단어 사전 경로
        category_files = {file.split('.')[0]: os.path.join(category_path, file) for file in os.listdir(category_path)}

        if not files:
            raise HTTPException(status_code=404, detail=f"No raw news found in '{raw_path}'")

        for file in files:
            file_year = extract_year_from_filename(file)  # 📌 연도 추출

            # 🛑 연도를 찾지 못하면 스킵
            if file_year is None:
                print(f"⏩ Skipping {file} (Year: None)")
                continue

            # 🛑 연도가 선택된 years 리스트에 없으면 스킵
            if selected_years and file_year not in selected_years:
                print(f"⏩ Skipping {file} (Year: {file_year})")
                continue

            print(f"✅ Processing {file} (Year: {file_year})")

            df = pd.read_csv(file, encoding='utf-8-sig', on_bad_lines="skip")
            try:
                _ = run_preprocess_naver(df)
                processed_news = run_semi_tfidf(_, category_files)
                
                if processed_news is not None:
                    if isinstance(processed_news, tuple):
                        processed_news = processed_news[0]
                    
                    # ✅ `_id`를 `Link` 값으로 설정하여 중복 방지
                    records = [{**record, "_id": record["Link"]} for record in processed_news.to_dict('records')]

                    saved_count = 0  # 저장된 데이터 개수
                    duplicate_count = 0  # 중복으로 저장되지 않은 데이터 개수

                    # ✅ tqdm으로 저장 진행률 표시 & 중복 방지 처리
                    for record in tqdm(records, desc=f"Saving {file} to MongoDB", unit="doc"):
                        existing_count = preprocessed_news_collection.count_documents({"_id": record["_id"]})

                        if existing_count == 0:
                            preprocessed_news_collection.insert_one(record)
                            saved_count += 1
                        else:
                            duplicate_count += 1  # 중복 데이터 개수 증가

                    print(f"✅ {file} processed: 총 {len(df)}개 중 {saved_count}개 저장됨, {duplicate_count}개 중복으로 저장 안됨")
                else:
                    print(f"⚠️ {file} processed: 전처리된 데이터 없음")
            except Exception as e:
                print(f"⚠️ {file} 처리 중 오류 발생: {e}")

    elif mode == "crawler":

        category_path = os.path.join(os.getcwd(), "Database", "sector_vocab")  # 산업별 단어 사전 경로
        category_files = {file.split('.')[0]: os.path.join(category_path, file) for file in os.listdir(category_path)}

        # ✅ 크롤러 실행 및 MongoDB 저장
        print("🚀 크롤러를 실행하여 네이버 뉴스를 수집합니다...")
        run_crawler(save_to_db=True)

        raw_news_collection = mongo_db.raw_news

        # ✅ DB 반영 대기 (최대 30초)
        timeout = 30
        elapsed = 0
        while raw_news_collection.count_documents({}) == 0 and elapsed < timeout:
            print("⏳ Waiting for MongoDB to reflect raw_news data...")
            time.sleep(2)
            elapsed += 2

        if raw_news_collection.count_documents({}) == 0:
            raise HTTPException(status_code=500, detail="⛔ 크롤러 실행 후에도 raw_news에 데이터가 없음")

        # ✅ MongoDB에서 `raw_news` 가져오기
        raw_data = list(raw_news_collection.find())

        if not raw_data:
            raise HTTPException(status_code=500, detail="⛔ raw_news에서 데이터를 가져오지 못함")

        print(f"✅ raw_news 데이터 개수: {len(raw_data)}개")

        # ✅ `_id`를 `Link` 값으로 설정하여 중복 방지
        for record in raw_data:
            record["_id"] = record["Link"]

        # ✅ 전처리 및 TF-IDF 적용
        _ = run_preprocess_naver(pd.DataFrame(raw_data))
        processed_news = run_semi_tfidf(_, category_files)

        if processed_news is not None:
            if isinstance(processed_news, tuple):
                processed_news = processed_news[0]

            records = [{**record, "_id": record["Link"]} for record in processed_news.to_dict('records')]

            saved_count, duplicate_count = 0, 0

            for record in tqdm(records, desc="Saving processed news to MongoDB", unit="doc"):
                if preprocessed_news_collection.count_documents({"_id": record["_id"]}) == 0:
                    preprocessed_news_collection.insert_one(record)
                    saved_count += 1
                else:
                    duplicate_count += 1

            print(f"✅ 전처리 완료: 총 {len(raw_data)}개 중 {saved_count}개 저장됨, {duplicate_count}개 중복으로 저장 안됨")

        # ✅ raw_news 컬렉션 삭제
        print("🗑️ Deleting raw_news collection...")
        raw_news_collection.drop()
        print("✅ raw_news 컬렉션 삭제 완료!")

    else:
        raise HTTPException(status_code=400, detail="잘못된 모드입니다. 'local' 또는 'crawler'를 선택하세요.")

    return {"message": f"✅ News processing completed using mode: {mode}, years: {selected_years}"}