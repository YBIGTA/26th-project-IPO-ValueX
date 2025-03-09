import pandas as pd

def run_merge_date_data(df_daily_csv="df_daily.csv", df_monthly_csv="df_monthly.csv", output_csv="df_merged.csv"):
    df_daily = pd.read_csv(df_daily_csv, encoding="utf-8-sig")
    df_monthly = pd.read_csv(df_monthly_csv, encoding="utf-8-sig")

    df_merged = df_daily.merge(df_monthly, on="month_key", how="left")

    # df_merged.to_csv(output_csv, encoding="utf-8-sig", index=False)
    print(f"✅ {output_csv} 생성 완료!")

    return df_merged

# if __name__ == "__main__":
#     run_merge_date_data()