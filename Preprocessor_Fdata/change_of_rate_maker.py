import pandas as pd
import os

def calculate_daily_change_rate(df_daily):
    # 숫자형 데이터만 선택
    df_daily_numeric = df_daily.select_dtypes(include=['number'])

    # 14일 전과 비교한 변화율 계산
    df_daily_change_rate = df_daily_numeric.pct_change(periods=14) * 100

    # 날짜 컬럼 추가
    df_daily_change_rate.insert(0, "date", df_daily["date"])

    # 공란(NaN)이 있을 경우, 다음 행의 값으로 채우기
    df_daily_change_rate.fillna(method="bfill", inplace=True)

    return df_daily_change_rate

def calculate_monthly_change_rate(df_monthly):
    # 산업생산지수 삭제 및 중복 제거
    df_monthly = df_monthly.drop(columns=["산업생산지수(IPI)"], errors="ignore")
    df_monthly = df_monthly.drop_duplicates()

    # 숫자형 데이터만 선택
    df_monthly_numeric = df_monthly.select_dtypes(include=['number'])

    # 1개월 전과 비교한 변화율 계산
    df_monthly_change_rate = df_monthly_numeric.pct_change(periods=1) * 100

    # 날짜 컬럼 추가
    df_monthly_change_rate.insert(0, "month_key", df_monthly["month_key"].dt.strftime('%Y-%m'))

    # 공란(NaN)이 있을 경우, 다음 행의 값으로 채우기
    df_monthly_change_rate.fillna(method="bfill", inplace=True)

    return df_monthly_change_rate

if __name__ == "__main__":
    # 파일 경로 설정
    base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "Finance_data"))
    daily_file_path = os.path.join(base_path, "df_daily_new.csv")
    monthly_file_path = os.path.join(base_path, "df_monthly_new.csv")

    # 파일 로드
    df_daily = pd.read_csv(daily_file_path)
    df_monthly = pd.read_csv(monthly_file_path)

    # 날짜 컬럼을 datetime 형식으로 변환
    df_daily["date"] = pd.to_datetime(df_daily["date"])
    df_monthly["month_key"] = pd.to_datetime(df_monthly["month_key"], errors='coerce')

    # 변환 실패한 경우 처리 (예: Jan-14 같은 값)
    df_monthly["month_key"] = pd.to_datetime(df_monthly["month_key"], format='%b-%y', errors='coerce')

    # 변화율 계산
    df_daily_change_rate = calculate_daily_change_rate(df_daily)
    df_monthly_change_rate = calculate_monthly_change_rate(df_monthly)

    # 결과 저장
    df_daily_change_rate.to_csv(os.path.join(base_path, "daily_new_change_rate.csv"), index=False)
    df_monthly_change_rate.to_csv(os.path.join(base_path, "monthly_new_change_rate.csv"), index=False)

    print("✅ 14일 및 1개월 변화율이 계산되고 저장되었습니다.")
