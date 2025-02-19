# import json
# from datetime import datetime

# def split_json_by_year(input_file):
#     # 파일 로드
#     with open(input_file, "r", encoding="utf-8") as f:
#         data = json.load(f)

#     # 분류할 딕셔너리 초기화
#     grouped_data = {
#         "2523": [],
#         "2220": [],
#         "1917": [],
#         "1614": []
#     }

#     # 데이터 분류
#     for entry in data:
#         listing_date = entry.get("상장일")
#         if not listing_date:
#             continue
        
#         year = datetime.strptime(listing_date, "%Y-%m-%d").year

#         if 2023 <= year <= 2025:
#             grouped_data["2523"].append(entry)
#         elif 2020 <= year <= 2022:
#             grouped_data["2220"].append(entry)
#         elif 2017 <= year <= 2019:
#             grouped_data["1917"].append(entry)
#         elif 2014 <= year <= 2016:
#             grouped_data["1614"].append(entry)

#     # 파일 저장
#     for group, entries in grouped_data.items():
#         output_file = f"KIND_Final_{group}.json"
#         with open(output_file, "w", encoding="utf-8") as f:
#             json.dump(entries, f, ensure_ascii=False, indent=4)
#         print(f"File saved: {output_file}")

# # 실행
# input_file = "./Finance_data/KIND_Final.json"  # 기존 JSON 파일명
# split_json_by_year(input_file)

import json
from datetime import datetime
import os

def split_json_by_year(input_file):
    # 파일 로드
    with open(input_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    # 분류할 딕셔너리 초기화
    grouped_data = {
        "2523": [],
        "2220": [],
        "1917": [],
        "1614": []
    }

    # 데이터 분류
    for entry in data:
        listing_date = entry.get("상장일")
        if not listing_date:
            continue
        
        try:
            year = datetime.strptime(listing_date, "%Y-%m-%d").year
        except ValueError:
            continue  # 날짜 형식이 잘못된 경우 무시

        if 2023 <= year <= 2025:
            grouped_data["2523"].append(entry)
        elif 2020 <= year <= 2022:
            grouped_data["2220"].append(entry)
        elif 2017 <= year <= 2019:
            grouped_data["1917"].append(entry)
        elif 2014 <= year <= 2016:
            grouped_data["1614"].append(entry)

    # 파일 저장 (상장일 기준 내림차순 정렬 후)
    for group, entries in grouped_data.items():
        sorted_entries = sorted(entries, key=lambda x: x["상장일"], reverse=True)  # 내림차순 정렬
        output_file = f"KIND_Final_{group}.json"
        output_dir=os.path.join("./Finance_data/",output_file)

        with open(output_dir, "w", encoding="utf-8") as f:
            json.dump(sorted_entries, f, ensure_ascii=False, indent=4)

        print(f"✅ File saved: {output_file}")

# 실행
input_file = "./Finance_data/KIND_Final.json"  # 기존 JSON 파일명
split_json_by_year(input_file)
