import json
import pandas as pd
from datetime import datetime
import os

def load_json(file_path):
    with open(file_path, "r", encoding="utf-8-sig") as f:
        data = json.load(f)
    return data

def calculate_weighted_sentiment(data):
    grouped_data = {}
    
    for entry in data:
        company = entry["기업명"]
        listing_date = datetime.strptime(entry["상장일"], "%Y-%m-%d")
        record_date = datetime.strptime(entry["날짜"], "%Y-%m-%d")
        sentiment_score = entry["최종매매감정점수"]
        
        # 가중치 계산 (상장일 - 날짜가 작을수록 높은 가중치)
        days_diff = (listing_date - record_date).days + 1  # +1을 추가하여 0일이 되지 않게 함
        weight = 1 / days_diff
        
        if company not in grouped_data:
            grouped_data[company] = {"weighted_sum": 0, "weight_sum": 0}
        
        grouped_data[company]["weighted_sum"] += sentiment_score * weight
        grouped_data[company]["weight_sum"] += weight
    
    # 가중평균 계산
    result = []
    for company, values in grouped_data.items():
        weighted_avg = values["weighted_sum"] / values["weight_sum"]
        result.append({"기업명": company, "대표매매감정점수": round(weighted_avg, 6)})
    
    return pd.DataFrame(result)

def main():
    file_path = os.path.join(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "Non_Finance_data")), "38","sentiment_38","kf_deberta_kote_final_ver2.json")
    data = load_json(file_path)
    df_result = calculate_weighted_sentiment(data)
    df_result.to_csv("company_weighted_sentiment.csv", index=False, encoding="utf-8-sig")
    print("기업별 가중평균 매매감정점수 저장 완료: company_weighted_sentiment.csv")

if __name__ == "__main__":
    main()
