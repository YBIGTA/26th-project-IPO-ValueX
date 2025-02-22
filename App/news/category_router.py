import os
import pandas as pd
from tqdm import tqdm
from datetime import datetime
from fastapi import APIRouter, HTTPException
from Database.mongodb_connection import mongo_db
from LLM_modeling.vector_to_category.single_processor import single_processor

router = APIRouter(
    prefix="/category",
    tags=["category"]
)

@router.post("/score/category")
def score_category():
    """
    날짜별 카테고리 점수를 계산하여 DB에 저장합니다.
    
    1. DB에서 마지막 저장 날짜를 확인합니다.
    2. 마지막 날짜가 2025-02-19 이전이면, 로컬 CSV 파일에서 데이터를 처리합니다.
    3. 마지막 날짜가 2025-02-19 이상이면, DB에서 최신 데이터를 가져와 single_processor로 처리합니다.
    4. 2014-01-15 이후의 날짜에 대해 지난 14일 간의 가중 평균을 계산합니다.
    """
    collection = mongo_db.category_score
    last_date_entry = collection.find_one({}, sort=[("Date", -1)])

    if last_date_entry:
        try:
            last_date = datetime.strptime(last_date_entry["Date"], "%Y-%m-%d").date()
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error parsing last date from DB: {e}")
    else:
        last_date = datetime(2013, 12, 31).date()

    if last_date < datetime(2025, 2, 19).date():
        start_date = last_date + pd.Timedelta(days=1)
        end_date = datetime(2025, 2, 19).date()
    else:
        start_date = last_date + pd.Timedelta(days=1)
        end_date = datetime.now().date()

    date_list = [start_date + pd.Timedelta(days=i) for i in range((end_date - start_date).days + 1)]

    for target_date in tqdm(date_list, desc="Processing dates"):
        target_date_str = target_date.strftime("%Y-%m-%d")
        category_scores = None
        try:
            year = target_date.year
            file_path = os.path.join(".", "LLM_modeling", "vector_to_category", "output", f"cat_{year}.csv")

            if os.path.exists(file_path):
                df = pd.read_csv(file_path)
                df['date'] = pd.to_datetime(df['date'])
                daily_df = df[df['date'].dt.date == target_date]
            else:
                daily_df = pd.DataFrame()

            if not daily_df.empty:
                category_scores = daily_df.drop(columns=['date', 'link'], errors='ignore').mean().to_dict()
            else:
                # 로컬 파일에 데이터가 없으면, 이전 날의 데이터를 보간합니다.
                prev_date = target_date - pd.Timedelta(days=1)
                prev_year = prev_date.year
                prev_file_path = os.path.join(".", "LLM_modeling", "vector_to_category", "output", f"cat_{prev_year}.csv")

                if os.path.exists(prev_file_path):
                    prev_df = pd.read_csv(prev_file_path)
                    prev_df['date'] = pd.to_datetime(prev_df['date'])
                    prev_daily_df = prev_df[prev_df['date'].dt.date == prev_date]
                    if not prev_daily_df.empty:
                        category_scores = prev_daily_df.drop(columns=['date', 'link'], errors='ignore').mean().to_dict()
                    else:
                        raise HTTPException(status_code=400, detail=f"No data to interpolate with for {target_date_str}.")
                else:
                    raise HTTPException(status_code=400, detail=f"No previous file available for interpolation for {target_date_str}.")

        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error processing {target_date_str}: {e}")

        # 2025-02-20 이후 날짜: DB의 news_data에서 가져온 데이터로 single_processor를 실행합니다.
        if target_date >= datetime(2025, 2, 20).date():
            new_data = list(mongo_db.preprocessed_news.find({
                "Date": target_date.strftime("%Y%m%d")
            }))
            if new_data:
                try:
                    processor_result = single_processor(new_data)
                except Exception as e:
                    raise HTTPException(status_code=500, detail=f"Error in single_processor for {target_date_str}: {e}")
                if processor_result is not None:
                    category_scores = processor_result
                else:
                    raise HTTPException(status_code=400, detail=f"single_processor returned None for date {target_date_str}.")
            else:
                # 해당 날짜에 처리할 데이터가 없으면 건너뜁니다.
                continue
        
        weighted_avg_scores = None
        if target_date >= datetime(2014, 1, 15).date():
            try:
                weighted_avg_scores = calculate_weighted_average(collection, target_date)
            except Exception as e:
                raise HTTPException(status_code=500, detail=f"Error calculating weighted average for {target_date_str}: {e}")
        key = ["성장_긍정", "성장_부정", "민감_긍정", "민감_부정",
               "방어_긍정", "방어_부정", "금융_긍정", "금융_부정"]
        category_scores = {key[i]:category_scores[i] for i in range(len(key))}
        data_to_insert = {
            "Date": target_date_str,
            "category_scores": category_scores,
            "weighted_avg_scores": weighted_avg_scores
        }
        try:
            collection.insert_one(data_to_insert)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to insert data for {target_date_str}: {e}")

    return {"message": "Category score calculation and DB posting completed."}

def calculate_weighted_average(collection, target_date):
    fourteen_days_ago = target_date - pd.Timedelta(days=14)
    daily_scores = list(collection.find({
        "Date": {"$gte": fourteen_days_ago.strftime("%Y-%m-%d"),
                 "$lt": target_date.strftime("%Y-%m-%d")}
    }))
    
    if not daily_scores:
        return None

    daily_scores = sorted(daily_scores, key=lambda x: x["Date"])

    weighted_avg = {}
    categories = daily_scores[0]["category_scores"].keys()
    for category in categories:
        scores = [day["category_scores"].get(category, 0) for day in daily_scores]
        weights = list(range(1, len(scores) + 1))
        weighted_sum = sum(s * w for s, w in zip(scores, weights))
        total_weight = sum(weights)
        weighted_avg[category] = weighted_sum / total_weight if total_weight > 0 else 0

    return weighted_avg