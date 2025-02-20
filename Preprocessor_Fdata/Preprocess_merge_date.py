import pandas as pd

def merge_date_data(df_daily_csv="df_daily.csv", df_monthly_csv="df_monthly.csv", df_yearly_csv="X_stat.csv", output_csv="df_final.csv"):
    df_daily = pd.read_csv(df_daily_csv, encoding="utf-8-sig")
    df_monthly = pd.read_csv(df_monthly_csv, encoding="utf-8-sig")
    df_yearly = pd.read_csv(df_yearly_csv, encoding="utf-8-sig")

    df_merged = df_daily.merge(df_monthly, on="month_key", how="left")
    df_final = df_merged.merge(df_yearly, on="year_key", how="left")

    df_final.to_csv(output_csv, encoding="utf-8-sig", index=False)
    print(f"✅ {output_csv} 생성 완료!")

if __name__ == "__main__":
    merge_date_data()