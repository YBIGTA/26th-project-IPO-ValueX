import pandas as pd
import json
import os

def process_daily(output_csv="df_daily.csv"):
    file_names = [
        "KOSPI_1001.json", "KOSPI_1005.json", "KOSPI_1008.json", 
        "KOSPI_1011.json", "KOSPI_1012.json", "KOSPI_1013.json",
        "KOSPI_1014.json", "KOSPI_1015.json", "KOSPI_1017.json",
        "KOSPI_1018.json", "KOSPI_1020.json", "KOSPI_1021.json",
        "KOSPI_1024.json", "KOSPI_1026.json", "KOSPI_1035.json"
    ]

    merged_df = pd.DataFrame()

    for file_name in file_names:
        with open(file_name, "r", encoding="utf-8") as f:
            data = json.load(f)

        df = pd.DataFrame(data)[["날짜", "종가"]]
        stock_name = file_name.replace(".json", "")
        df = df.rename(columns={"종가": stock_name})

        if merged_df.empty:
            merged_df = df
        else:
            merged_df = pd.merge(merged_df, df, on="날짜", how="outer")

    merged_df = merged_df.sort_values(by="날짜").reset_index(drop=True)
    merged_df["date"] = merged_df["날짜"]
    merged_df["year_key"] = pd.to_datetime(merged_df["date"]).dt.year
    merged_df["month_key"] = pd.to_datetime(merged_df["date"]).dt.strftime("%Y-%m")

    merged_df.to_csv(output_csv, index=False, encoding="utf-8-sig")
    print(f"✅ {output_csv} 생성 완료!")

if __name__ == "__main__":
    process_daily()