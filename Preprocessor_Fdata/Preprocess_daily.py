import pandas as pd
import json
import os

def run_process_daily(output_csv="df_daily_new.csv"):
    file_names = [
        "KOSPI_new1001.json", "KOSPI_new1005.json","KOSPI_new1008.json","KOSPI_new1011.json","KOSPI_new1012.json", "KOSPI_new1013.json", 
        "KOSPI_new1014.json","KOSPI_new1015.json","KOSPI_new1017.json","KOSPI_new1018.json","KOSPI_new1020.json","KOSPI_new1021.json",
        "KOSPI_new1024.json","KOSPI_new1026.json","KOSPI_new1035.json","KOSPI_new1045.json","KOSPI_new1155.json","KOSPI_new1156.json","KOSPI_new2001.json"
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

    return merged_df


if __name__ == "__main__":
     run_process_daily()