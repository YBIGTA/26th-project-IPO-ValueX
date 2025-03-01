import pandas as pd
import os
import json

def load_data():
    base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "Finance_data"))
    file_path = os.path.join(base_path, "X_stat.csv")
    
    return_ratio_path = os.path.join(base_path, "etc", "IPOSTOCK_new.json")
    
    sentiment_path = "new_company_weighted_sentiment.csv"
    daily_path = os.path.join(base_path, "df_daily.csv")
    monthly_path = os.path.join(base_path, "df_monthly.csv")
    sector_info_path = os.path.join(base_path, "etc", "섹터별분류.csv")

    df = pd.read_csv(file_path)
    df_monthly_raw = pd.read_csv(monthly_path)
    df_daily_raw = pd.read_csv(daily_path, encoding="utf-8-sig")  # 인코딩 명시
    df_sentiment = pd.read_csv(sentiment_path)
    df_sector = pd.read_csv(sector_info_path)

    # JSON 데이터 로드
    with open(return_ratio_path, "r", encoding="utf-8-sig") as f:
        return_ratio_data = json.load(f)

    extracted_data = []
    if isinstance(return_ratio_data, list):
        for entry in return_ratio_data:
            for company, data in entry.items():
                종가대비등락율 = data.get("종가대비등락율", None)
                extracted_data.append({"기업명": company, "종가대비등락율": 종가대비등락율})
    else:
        for company, data in return_ratio_data.items():
            종가대비등락율 = data.get("종가대비등락율", None)
            extracted_data.append({"기업명": company, "종가대비등락율": 종가대비등락율})

    df_return_ratio = pd.DataFrame(extracted_data)

    return df, df_monthly_raw, df_daily_raw, df_return_ratio, df_sentiment, df_sector

def preprocess_dates(df, df_daily_raw):
    date_columns = ["수요예측 시작일", "수요예측 종료일", "상장일"]
    for col in date_columns:
        df[col] = pd.to_datetime(df[col], errors='coerce')

    if "date" in df_daily_raw.columns:
        df_daily_raw["date"] = pd.to_datetime(df_daily_raw["date"], errors='coerce')
    else:
        print("⚠️ Warning: 'date' column is missing in df_daily_volatility!")

    return df, df_daily_raw

def calculate_dlr(df):
    df["DLT"] = (df["상장일"] - df["수요예측 시작일"]).dt.days
    df["DLG"] = (df["상장일"] - df["수요예측 종료일"]).dt.days
    df["DLR"] = (df["DLG"] / df["DLT"]) * 100
    df["DLR"] = df["DLR"].round(2)
    return df

def calculate_ppi(df):
    df["(확정)공모가격"] = df["(확정)공모가격"].str.replace(",", "").astype(float)
    denominator = df["(희망)공모가격 상한가"] - df["(희망)공모가격 하한가"]
    ppi_valid = (denominator != 0) & (~denominator.isna()) & (~df["(확정)공모가격"].isna())

    df.loc[ppi_valid, "PPI (%)"] = ((df.loc[ppi_valid, "(확정)공모가격"] - df.loc[ppi_valid, "(희망)공모가격 하한가"]) / denominator) * 100

    df["PPI (%)"] = df["PPI (%)"].ffill().bfill().round(2)

    print(f"PPI NaN 개수: {df['PPI (%)'].isnull().sum()}")

    return df

def merge_changerate_data(df, df_monthly_raw, df_daily_raw):
    # 🔹 상장일을 강제로 datetime64[ns] 형식으로 변환
    df["상장일"] = pd.to_datetime(df["상장일"], errors="coerce")

    # 🔹 df_monthly_change의 month_key 변환
    df_monthly_raw["month_key"] = pd.to_datetime(df_monthly_raw["month_key"], format="%Y-%m", errors="coerce").dt.strftime('%b_%y')
    df["month_key"] = df["상장일"].dt.strftime('%b_%y')

    # 🔹 월별 변동성 데이터 병합
    df = df.merge(df_monthly_raw, on="month_key", how="left")

    # 🔹 df_daily_change의 date 변환
    if "date" in df_daily_raw.columns:
        df_daily_raw["date"] = pd.to_datetime(df_daily_raw["date"], errors="coerce")

        # 🔹 데이터 타입 확인 (디버깅)
        print(f"상장일 타입: {df['상장일'].dtype}, df_daily_raw['date'] 타입: {df_daily_raw['date'].dtype}")

        # 🔹 병합 수행
        df = df.merge(df_daily_raw[["date", "KOSPI_1001"]], left_on="상장일", right_on="date", how="left")
        df = df.drop(columns=["date"])  # 병합 후 중복 컬럼 제거
    else:
        print("⚠️ Warning: 'date' column is missing in df_daily_change!")

    # 🔹 국고채 금리 스프레드 계산 및 컬럼 정리
    df["국고채 금리 스프레드"] = df["국고채 금리 (10년)"] - df["국고채 금리 (3년)"]
    df = df.drop(columns=["수요예측 시작일", "수요예측 종료일", "(희망)공모가격 상한가", "(희망)공모가격 하한가", "DLT", "DLG", "국고채 금리 (10년)", "국고채 금리 (3년)"])

    return df


def merge_return_ratio(df, df_return_ratio):
    df["종가대비등락율"] = df["기업명"].map(df_return_ratio.set_index("기업명")["종가대비등락율"])
    return df

def merge_sentiment_data(df, df_sentiment):
    df["대표매매감정점수"] = df["기업명"].map(df_sentiment.set_index("기업명")["대표매매감정점수"])
    return df

def merge_sector_data(df, df_sector):
    df_sector = df_sector[["기업명", "산업군", "섹터"]]
    df = df.merge(df_sector, on="기업명", how="left")
    return df

def save_data(df):
    df.to_csv("df_regression_raw.csv", index=False)
    print("DLR 및 PPI 계산 완료. 데이터가 병합되었으며, 'df_regression_version.csv'로 저장됨.")

def main():
    df, df_monthly_raw, df_daily_raw, df_return_ratio, df_sentiment, df_sector = load_data()
     # 상장일을 날짜 형식으로 변환 (미리 적용)
    df["상장일"] = pd.to_datetime(df["상장일"], errors="coerce")
    df, df_daily_raw = preprocess_dates(df, df_daily_raw)
    df = calculate_dlr(df)
    df = calculate_ppi(df)
    df = merge_changerate_data(df, df_monthly_raw, df_daily_raw)
    df = merge_return_ratio(df, df_return_ratio)
    df = merge_sentiment_data(df, df_sentiment)
    df = merge_sector_data(df, df_sector)
    save_data(df)

if __name__ == "__main__":
    main()