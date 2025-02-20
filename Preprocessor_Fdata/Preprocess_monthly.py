import pandas as pd
import json
import os

def process_monthly(output_csv="df_monthly.csv"):
    file_names = [
        "KOREA_국고채_금리_(3년).json", "KOREA_국고채_금리_(10년).json", 
        "KOREA_기업경기실사지수(BSI).json", "KOREA_기준금리.json",
        "KOREA_무역수지.json", "KOREA_산업생산지수(IPI).json",
        "KOREA_소비심리지수(CSI).json", "KOREA_소비자물가_상승률_(CPI).json",
        "KOREA_외환보유액.json", "KOREA_원달러_환율.json", "KOREA_통화량_(M2).json"
    ]

    merged_df = pd.DataFrame()

    for file_name in file_names:
        with open(file_name, "r", encoding="utf-8-sig") as f:
            data = json.load(f)

        df = pd.DataFrame(data)

        if merged_df.empty:
            merged_df = df
        else:
            merged_df = pd.merge(merged_df, df, on="날짜", how="outer")

    merged_df = merged_df.sort_values(by="날짜").reset_index(drop=True)
    merged_df["month_key"] = pd.to_datetime(merged_df["날짜"], format="%Y%m").dt.strftime("%Y-%m")
    merged_df.to_csv(output_csv, index=False, encoding="utf-8-sig")
    print(f"✅ {output_csv} 생성 완료!")

if __name__ == "__main__":
    process_monthly()