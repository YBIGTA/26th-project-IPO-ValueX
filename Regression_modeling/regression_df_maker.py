import pandas as pd
import os
import json

def load_data():
    base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "Finance_data"))
    file_path = os.path.join(base_path, "X_stat.csv")
    monthly_volatility_path = os.path.join(base_path, "monthly_volatility.csv")
    return_ratio_path = os.path.join(base_path, "etc", "IPOSTOCK_data.json")
    daily_volatility_path = os.path.join(base_path, "daily_volatility.csv")
    
    df = pd.read_csv(file_path)
    df_monthly_volatility = pd.read_csv(monthly_volatility_path)
    df_daily_volatility = pd.read_csv(daily_volatility_path)
    
    with open(return_ratio_path, "r", encoding="utf-8-sig") as f:
        return_ratio_data = json.load(f)
    
    # JSON 데이터가 리스트인 경우 처리
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
    return df, df_monthly_volatility, df_daily_volatility, df_return_ratio

def preprocess_dates(df, df_daily_volatility):
    date_columns = ["수요예측 시작일", "수요예측 종료일", "상장일"]
    for col in date_columns:
        df[col] = pd.to_datetime(df[col], errors='coerce')
    df_daily_volatility["date"] = pd.to_datetime(df_daily_volatility["date"], errors='coerce')
    return df, df_daily_volatility

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

def merge_volatility_data(df, df_monthly_volatility, df_daily_volatility):
    df_monthly_volatility["month_key"] = pd.to_datetime(df_monthly_volatility["month_key"], format="%Y-%m", errors="coerce").dt.strftime('%b_%y')
    df["month_key"] = df["상장일"].dt.strftime('%b_%y')
    df = df.merge(df_monthly_volatility, on="month_key", how="left")
    df = df.merge(df_daily_volatility[["date", "KOSPI_1001"]], left_on="상장일", right_on="date", how="left")
    df = df.drop(columns=["date"])
    
    # 국고채 금리 스프레드 계산 및 추가
    df["국고채 금리 스프레드"] = df["국고채 금리 (10년)"] - df["국고채 금리 (3년)"]

    df = df.drop(columns=["수요예측 시작일", "수요예측 종료일", "(희망)공모가격 상한가", "(희망)공모가격 하한가", "DLT","DLG","국고채 금리 (10년)",  "국고채 금리 (3년)"])
    
    return df

def merge_return_ratio(df, df_return_ratio):
    df["종가대비등락율"] = df["기업명"].map(df_return_ratio.set_index("기업명")["종가대비등락율"])
    
    # 컬럼 순서 재조정
    column_order = ["기업명", "year_key", "month_key", "단순 기관경쟁률", "의무보유확약비율", "(확정)공모가격", "청약경쟁률", "상장일", "공모후 발행주식수", 
                    "유동자산", "비유동자산", "유동부채", "비유동부채", "자본금", "이익잉여금", "기타자본항목", "영업이익", "당기순이익", "매출액", 
                    "부채비율", "유동비율", "영업이익률", "당기순이익률", "ROE", "EPS", "EV/영업이익", "최대주주 소유주 비율", "최대주주 보호예수 기간", 
                    "보호예수 비율", "부채비율 변화율", "유동비율 변화율", "영업이익률 변화율", "당기순이익률 변화율", "매출액 변화율", "ROE 변화율", "EPS 변화율", 
                    "EV/영업이익 변화율", "DLR", "PPI (%)", "기업경기실사지수(BSI)", "기준금리", "무역수지", "생산자물가 상승률 (PPI)", "소비심리지수(CSI)", 
                    "소비자물가 상승률 (CPI)", "수출입 물량지수", "외환보유액", "원달러 환율", "통화량 (M2)", "국고채 금리 스프레드", "KOSPI_1001", "종가대비등락율"]
    

    df = df[column_order]
    
    return df

def save_data(df):
    df.to_csv("df_regression.csv", index=False)
    print("DLR 및 PPI 계산 완료. monthly_volatility 및 daily_volatility 데이터가 병합되었으며, 'df_regression.csv'로 저장됨.")

def main():
    df, df_monthly_volatility, df_daily_volatility, df_return_ratio = load_data()
    df, df_daily_volatility = preprocess_dates(df, df_daily_volatility)
    df = calculate_dlr(df)
    df = calculate_ppi(df)
    df = merge_volatility_data(df, df_monthly_volatility, df_daily_volatility)
    df = merge_return_ratio(df, df_return_ratio)
    save_data(df)

if __name__ == "__main__":
    main()
