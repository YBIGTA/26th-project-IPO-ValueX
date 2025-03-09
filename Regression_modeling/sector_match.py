import os
import pandas as pd
import ast

# 📌 기존 코드에서 사용한 경로 유지
base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "Finance_data"))
regression_path = "df_regression_change.csv"
category_score_path = "category_score.csv"
daily_change_rate_path = os.path.join(base_path, "daily_change_rate.csv")

# 📌 데이터 로드
df_regression = pd.read_csv(regression_path)
df_category = pd.read_csv(category_score_path)
df_daily_change = pd.read_csv(daily_change_rate_path)

# ✅ 날짜 형식 변환 (normalize() 추가하여 비교 정확도 향상)
df_regression["상장일"] = pd.to_datetime(df_regression["상장일"]).dt.normalize()
df_category["Date"] = pd.to_datetime(df_category["Date"]).dt.normalize()
df_daily_change["date"] = pd.to_datetime(df_daily_change["date"]).dt.normalize()

# ✅ 영어 섹터명을 한국어 섹터명으로 변환하는 매핑
sector_mapping = {
    "Growth": "성장",
    "Sensitive": "민감",
    "Protective": "방어",
    "Infra": "금융"
}

# df_regression의 '섹터'를 한국어로 변환
df_regression["섹터_한글"] = df_regression["섹터"].map(sector_mapping)

# ✅ 뉴스 감성 점수 매핑 (weighted_avg_scores 사용)
df_regression["호황"] = None
df_regression["불황"] = None

# ✅ weighted_avg_scores 컬럼 변환 (문자열 → 딕셔너리로 변환)
if "weighted_avg_scores" in df_category.columns:
    def parse_weighted_avg_scores(value):
        if pd.isna(value):
            return {}  # NaN이면 빈 딕셔너리 반환
        try:
            parsed_dict = ast.literal_eval(value)  # 문자열을 딕셔너리로 변환
            processed_scores = {}

            # ✅ "섹터_긍정" & "섹터_부정"을 분리하여 저장
            for key, val in parsed_dict.items():
                sector, sentiment = key.rsplit("_", 1)  # 마지막 "_" 기준으로 나누기
                if sector not in processed_scores:
                    processed_scores[sector] = {"긍정": 0, "부정": 0}
                processed_scores[sector][sentiment] = val

            return processed_scores  # 변환된 딕셔너리 반환
        except (ValueError, SyntaxError):
            return {}  # 변환 실패 시 빈 딕셔너리 반환

    df_category["weighted_avg_scores"] = df_category["weighted_avg_scores"].apply(parse_weighted_avg_scores)

# ✅ 뉴스 감성 점수 매핑
for idx, row in df_regression.iterrows():
    ipo_date = row["상장일"]
    sector_kor = row["섹터_한글"]

    # 해당 상장일의 감성 점수 데이터 찾기
    matched_row = df_category[df_category["Date"] == ipo_date]

    if not matched_row.empty:
        score_dict = matched_row["weighted_avg_scores"].values[0]

        if isinstance(score_dict, dict) and sector_kor in score_dict:
            scores = score_dict.get(sector_kor, {})
            df_regression.at[idx, "호황"] = scores.get("긍정", 0)  # 긍정 점수
            df_regression.at[idx, "불황"] = -scores.get("부정", 0)  # 부정 점수 (음수 변환)

# ✅ KOSPI 산업군 매핑 사전
kospi_sector_mapping = {
    "KOSPI_1005": ["식품"],
    "KOSPI_1008": ["화학", "에너지", "환경"],
    "KOSPI_1011": ["철강", "금속"],
    "KOSPI_1012": ["기계제조", "부품제조", "로봇"],
    "KOSPI_1013": ["반도체", "배터리", "전자/회로", "가구/가전"],
    "KOSPI_1014": ["바이오", "의료/의약"],
    "KOSPI_1015": ["자동차", "선박", "항공", "방산", "자동차판매", "항공제조"],
    "KOSPI_1017": ["유통"],
    "KOSPI_1018": ["건설"],
    "KOSPI_1021": ["금융"],
    "KOSPI_1024": ["투자"],
    "KOSPI_1026": ["엔터", "게임", "서비스", "스포츠", "출판", "옷"],
    "KOSPI_1155": ["IT", "소프트웨어"]
}

# ✅ KOSPI 산업군 변동률 매칭
df_regression["산업코스피"] = None

for kospi, sectors in kospi_sector_mapping.items():
    matched_rows = df_regression["산업군"].isin(sectors)

    # 해당 섹터에 해당하는 상장일의 KOSPI 변화율 가져오기
    for idx in df_regression[matched_rows].index:
        ipo_date = df_regression.at[idx, "상장일"]

        # 해당 날짜의 KOSPI 변동률 찾기
        kospi_value = df_daily_change.loc[df_daily_change["date"] == ipo_date, kospi]

        # 값이 존재하면 추가
        if not kospi_value.empty:
            df_regression.at[idx, "산업코스피"] = kospi_value.values[0]

# ✅ 불필요한 임시 컬럼 제거
df_regression.drop(columns=["섹터_한글"], inplace=True)

# ✅ 기존 경로 유지하여 데이터 저장
output_path = "df_regression_change_sector.csv"
df_regression.to_csv(output_path, index=False, encoding="utf-8-sig")

print(f"✅ 업데이트 완료: '{output_path}' 저장됨")










