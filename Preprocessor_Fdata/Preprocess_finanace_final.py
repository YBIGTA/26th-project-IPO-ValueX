import pandas as pd

def run_merge_final_data(df_merged_csv="df_merged.csv", df_yearly_csv="X_stat.csv", output_csv="df_final.csv"):
    df_yearly = pd.read_csv(df_yearly_csv, encoding="utf-8-sig")

    df_merged = pd.read_csv(df_merged_csv, encoding="utf-8-sig")
    df_final = df_merged.merge(df_yearly, on="year_key", how="left")

    # df_final.to_csv(output_csv, encoding="utf-8-sig", index=False)
    print(f"✅ {output_csv} 생성 완료!")

    return df_final

# if __name__ == "__main__":
#     run_merge_final_data()