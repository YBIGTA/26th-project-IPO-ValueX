import openai
from Database.mongodb_connection import mongo_db
from fastapi import APIRouter, HTTPException, Query
from LLM_modeling.summary.summary import generate_summary
from LLM_modeling.vector_to_category.single_processor import single_processor

from datetime import datetime
import pandas as pd
import re
from dotenv import load_dotenv

load_dotenv()

router = APIRouter(
    prefix="/news",
    tags=["news"]
)

@router.post("/summarize/data")
def summarize_news(
    start_date: str = Query(default="20250218", description="시작 날짜 (YYYYMMDD)"),
    end_date: str = Query(default=datetime.now().strftime("%Y%m%d"), description="종료 날짜 (YYYYMMDD)")
):
    summarized_news = mongo_db.summarized_news
    news_data = mongo_db.preprocessed_news

    try:
        start_dt = datetime.strptime(start_date, "%Y%m%d").date()
        end_dt = datetime.strptime(end_date, "%Y%m%d").date()
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"날짜 형식 오류: {e}")

    date_list = [start_dt + pd.Timedelta(days=i) for i in range((end_dt - start_dt).days + 1)]
    inserted_count = 0

    for date in date_list:
        date_str = date.strftime("%Y%m%d")
        news_list = list(news_data.find({"$or": [{"Date": date_str}, {"Date": int(date_str)}]}))
        if not news_list:
            print(f"No news data in {date_str}. Skipping...")
            continue

        # 1. 인기 뉴스 선정 (Emotion 값의 합 + Num_comment 기준)
        popularity_scores = []
        for idx, news in enumerate(news_list):
            emotion_str = news.get("Emotion", "")
            emotion_sum = 0
            if emotion_str:
                parts = emotion_str.split()
                for part in parts:
                    try:
                        _, value = part.split(":")
                        emotion_sum += int(value)
                    except Exception:
                        continue
            try:
                num_comment = int(news.get("Num_comment", 0))
            except Exception:
                num_comment = 0

            score = emotion_sum + num_comment
            popularity_scores.append((idx, score))
        
        sorted_news = sorted(popularity_scores, key=lambda x: x[1], reverse=True)
        popular_index = [item[0] for item in sorted_news[:min(len(sorted_news), 10)]]
        popular_news = [news_list[i] for i in popular_index]

        # 2. 각 인기 뉴스에 대해 요약
        for news in popular_news:
            body_text = news.get("Body_processed", "")
            if not body_text:
                body_text = ("N/A")
                continue
            try:
                summary = generate_summary(body_text)
            except Exception as e:
                summary = f"Error: {e}"
                
            original_link = news.get("Link", "N/A")
            date = news.get("Date", "N/A")

            try:
                key = ["성장_긍정", "성장_부정", "민감_긍정", "민감_부정",
                       "방어_긍정", "방어_부정", "금융_긍정", "금융_부정"]
                category_scores = single_processor([news])
                category_scores = {key[i]:category_scores[i] for i in range(len(key))}
            except Exception as e:
                    raise HTTPException(
                        status_code=500,
                        detail=f"Error in single_processor for link <{original_link}>: {e}")
            
            doc = {
                "_id": original_link,
                "요약내용": summary,
                "날짜": str(date),
                "link": original_link,
                "사진": "",
                "카테고리점수": category_scores,
                "태그": ["", "", ""]
            }
            try:
                summarized_news.update_one({"_id": original_link}, {"$set": doc}, upsert=True)
                inserted_count += 1
            except Exception as e:
                raise HTTPException(status_code=500, detail=f"Failed to insert data for link <{original_link}>: {e}")
    
    return {"message": "News summarization completed.", "results_count": inserted_count}
