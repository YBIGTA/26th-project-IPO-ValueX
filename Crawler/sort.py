import json
import pandas as pd

# JSON 파일 불러오기
with open("./Finance_data/KIND_data.json", "r", encoding="utf-8") as file:
    data = json.load(file)

# DataFrame으로 변환
df = pd.DataFrame(data)

# 날짜 형식 변환 및 내림차순 정렬
df["상장일"] = pd.to_datetime(df["상장일"])
df_sorted = df.sort_values(by="상장일", ascending=False)  # 내림차순 정렬

# 상장일을 다시 원래 문자열 형식으로 변환
df_sorted["상장일"] = df_sorted["상장일"].dt.strftime("%Y-%m-%d")

# 정렬된 데이터 JSON 파일로 저장
df_sorted.to_json("sorted_KIND_data.json", orient="records", force_ascii=False, indent=4)


