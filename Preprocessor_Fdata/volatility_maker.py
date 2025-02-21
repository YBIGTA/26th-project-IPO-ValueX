import pandas as pd
import os

def calculate_daily_signed_volatility(df_daily):
    # 숫자형 데이터만 선택 후 수익률 계산
    df_daily_numeric = df_daily.select_dtypes(include=['number'])
    for col in df_daily_numeric.columns:
        df_daily_numeric[col] = df_daily_numeric[col].pct_change(fill_method=None)
    
    # 변동성(표준편차) 계산 (30일 이동 표준편차)
    df_daily_signed_volatility = df_daily_numeric.rolling(window=30, min_periods=1).std()
    
    # 부호 포함된 변동성 계산 (Signed Volatility)
    df_daily_sign = df_daily_numeric.rolling(window=30, min_periods=1).sum().apply(lambda x: x / abs(x), axis=0).fillna(1)
    df_daily_signed_volatility *= df_daily_sign
    
    # 날짜 컬럼 추가
    df_daily_signed_volatility.insert(0, "date", df_daily["date"])
    
    # 첫 번째 두 행의 키 유지하면서 값만 셋째 행과 동일하게 설정
    if len(df_daily_signed_volatility) > 2:
        df_daily_signed_volatility.iloc[0, 1:] = df_daily_signed_volatility.iloc[2, 1:]
        df_daily_signed_volatility.iloc[1, 1:] = df_daily_signed_volatility.iloc[2, 1:]
    
    return df_daily_signed_volatility

def calculate_monthly_signed_volatility(df_monthly):
    # 산업생산지수 삭제 및 중복 제거
    df_monthly = df_monthly.drop(columns=["산업생산지수(IPI)"])
    df_monthly = df_monthly.drop_duplicates()
    
    # 숫자형 데이터만 선택 후 수익률 계산
    df_monthly_numeric = df_monthly.select_dtypes(include=['number'])
    for col in df_monthly_numeric.columns:
        df_monthly_numeric[col] = df_monthly_numeric[col].pct_change(fill_method=None)
    
    # 월별 변동성 계산 (3개월 이동 표준편차)
    df_monthly_signed_volatility = df_monthly_numeric.rolling(window=3, min_periods=1).std()
    
    # 부호 포함된 변동성 계산 (Signed Volatility)
    df_monthly_sign = df_monthly_numeric.rolling(window=3, min_periods=1).sum().apply(lambda x: x / abs(x), axis=0).fillna(1)
    df_monthly_signed_volatility *= df_monthly_sign
    
    # 날짜 컬럼 추가
    df_monthly_signed_volatility.insert(0, "month_key", df_monthly["month_key"].dt.strftime('%Y-%m'))
    
    # 첫 번째 두 행의 키 유지하면서 값만 셋째 행과 동일하게 설정
    if len(df_monthly_signed_volatility) > 2:
        df_monthly_signed_volatility.iloc[0, 1:] = df_monthly_signed_volatility.iloc[2, 1:]
        df_monthly_signed_volatility.iloc[1, 1:] = df_monthly_signed_volatility.iloc[2, 1:]
    
    return df_monthly_signed_volatility

if __name__ == "__main__":
    # 파일 경로 설정
    base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "Finance_data"))
    daily_file_path = os.path.join(base_path, "df_daily.csv")
    monthly_file_path = os.path.join(base_path, "df_monthly.csv")
    
    # 파일 로드
    df_daily = pd.read_csv(daily_file_path)
    df_monthly = pd.read_csv(monthly_file_path)
    
    # 날짜 컬럼을 datetime 형식으로 변환
    df_daily["date"] = pd.to_datetime(df_daily["date"])
    df_monthly["month_key"] = pd.to_datetime(df_monthly["month_key"], errors='coerce')
    
    # 변환 실패한 경우 처리 (예: Jan-14 같은 값)
    df_monthly["month_key"] = pd.to_datetime(df_monthly["month_key"], format='%b-%y', errors='coerce')
    
    # 변동성 계산
    df_daily_signed_volatility = calculate_daily_signed_volatility(df_daily)
    df_monthly_signed_volatility = calculate_monthly_signed_volatility(df_monthly)
    
    # 결과 저장
    df_daily_signed_volatility.to_csv(os.path.join(base_path, "daily_volatility.csv"), index=False)
    df_monthly_signed_volatility.to_csv(os.path.join(base_path, "monthly_volatility.csv"), index=False)
    
    print("부호 포함된 일별 및 월별 변동성이 계산되고 저장되었습니다.")











