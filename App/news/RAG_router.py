import openai
from Database.mongodb_connection import mongo_db
from fastapi import APIRouter, HTTPException, Query
from LLM_modeling.RAG.RAG import generate_rag_response, generate_summary_and_keywords
from datetime import datetime
import pandas as pd
import re
from dotenv import load_dotenv

load_dotenv()

router = APIRouter(
    prefix="/summary",
    tags=["summary"]
)

@router.post("/summarize/data")
def summarize_news():
    summary_collection = mongo_db.summary_by_day
    news_data = mongo_db.preprocessed_news
    start_date = datetime(2025, 2, 18)
    end_date = datetime.now().date()
    date_list = [start_date + pd.Timedelta(days=i) for i in range((end_date - start_date).days + 1)]
    
    final_results = []  # 최종 결과 리스트 (요약, 추가 설명, 날짜, 링크 포함)
    
    for date in date_list:
        date_str = date.strftime("%Y%m%d")
        news_list = list(news_data.find({"Date": date_str}))
        
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
        popular_index = [item[0] for item in sorted_news[:3]]
        popular_news = [news_list[i] for i in popular_index]
        
        # 2. 각 인기 뉴스에 대해 요약과 키워드 추출
        summaries_and_keywords = []  # 각 요소는 string (예: "요약: ...\n키워드: 키워드1, 키워드2")
        for news in popular_news:
            body_text = news.get("Body_processed", "")
            if not body_text:
                summaries_and_keywords.append("No body content available.")
                continue
            try:
                summary_keywords = generate_summary_and_keywords(body_text)
                summaries_and_keywords.append(summary_keywords)
            except Exception as e:
                summaries_and_keywords.append(f"Error generating summary: {e}")
        
        # 3. 각 인기 뉴스에 대해 키워드를 기반으로 관련 뉴스 검색 및 RAG 실행
        for idx, summary_item in enumerate(summaries_and_keywords):
            # 추출된 결과에서 "키워드:" 이후의 내용을 파싱 (쉼표 구분)
            # 예시 결과: "요약: ...\n키워드: 경제, 성장"
            keyword_match = re.search(r"키워드:\s*(.+)", summary_item)
            if keyword_match:
                keywords_str = keyword_match.group(1)
                # 키워드를 쉼표로 분리하고 좌우 공백 제거
                keywords = [kw.strip() for kw in keywords_str.split(",")]
            else:
                keywords = []
            
            # 현재 뉴스의 Link (중복 검색 방지를 위해 본인 제외)
            original_link = popular_news[idx].get("Link", "N/A")
            
            retrieved_docs = []
            # 우선, 현재 날짜부터 3일간의 뉴스에서 키워드 포함 뉴스 검색 (본인 제외)
            for offset in range(0, 3):
                search_date = date + pd.Timedelta(days=offset)
                search_date_str = search_date.strftime("%Y%m%d")
                candidate_list = list(news_data.find({"Date": search_date_str}))
                for candidate in candidate_list:
                    if candidate.get("Link", "") == original_link:
                        continue  # 자기 자신 제외
                    candidate_body = candidate.get("Body_processed", "")
                    # 키워드 중 하나라도 포함되어 있으면 후보로 채택
                    if any(kw and kw in candidate_body for kw in keywords):
                        retrieved_docs.append(candidate_body)
            
            # 만약 관련 뉴스가 없다면, 2일 전으로 돌아가서 검색
            if not retrieved_docs:
                for offset in range(1, 3):
                    search_date = date - pd.Timedelta(days=offset)
                    search_date_str = search_date.strftime("%Y%m%d")
                    candidate_list = list(news_data.find({"Date": search_date_str}))
                    for candidate in candidate_list:
                        if candidate.get("Link", "") == original_link:
                            continue
                        candidate_body = candidate.get("Body_processed", "")
                        if any(kw and kw in candidate_body for kw in keywords):
                            retrieved_docs.append(candidate_body)
            
            # 만약 여전히 관련 뉴스가 없다면, RAG 결과는 None으로 처리
            if not retrieved_docs:
                rag_explanation = None
            else:
                try:
                    rag_explanation = generate_rag_response(summary_item, retrieved_docs, market_indicators=None)
                except Exception as e:
                    rag_explanation = f"Error generating RAG response: {e}"
            
            # 최종 결과 항목: 요약/키워드, 추가 설명, 날짜, 링크
            result_item = {
                "summary": summary_item,
                "rag_explanation": rag_explanation,
                "date": date_str,
                "link": original_link
            }
            final_results.append(result_item)
    
    # DB에 저장할 문서 구성 (여러 날짜에 걸친 결과 저장)
    summary_doc = {
        "generated_at": datetime.now().strftime("%Y%m%d%H%M%S"),
        "results": final_results
    }
    try:
        summary_collection.insert_one(summary_doc)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to insert summary document: {e}")
    
    return {"message": "News summarization and RAG process completed.", "results_count": len(final_results)}
