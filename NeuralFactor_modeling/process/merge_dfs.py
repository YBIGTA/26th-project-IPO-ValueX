import pandas as pd
import os
import ast  # 문자열 형태의 딕셔너리를 변환할 때 사용

# ✅ 실행 중인 스크립트의 디렉토리 가져오기
script_dir = os.path.dirname(os.path.abspath(__file__))

# ✅ 파일 경로 설정
finance_data_dir = os.path.join(os.path.expanduser("~"), "Desktop", "26th-project-IPO-ValueX", "Finance_data")
df_merged_path = os.path.join(finance_data_dir, "df_merged.csv")  # df_merged.csv 위치 변경
category_score_path = os.path.join(script_dir, "category_score.csv")  # 실행 파일과 같은 디렉토리
output_path = os.path.join(script_dir, "df_merged_with_category_scores.csv")  # 저장 경로 설정

# ✅ 데이터 불러오기
df_merged = pd.read_csv(df_merged_path, parse_dates=["date"])

# ✅ `category_score.csv` 컬럼명 확인 후 로드
category_score = pd.read_csv(category_score_path)
category_score.columns = category_score.columns.str.lower()  # 컬럼명을 소문자로 변환

# ✅ `date` 컬럼이 있는지 확인
if "date" not in category_score.columns:
    raise ValueError("❌ `category_score.csv` 파일에 'date' 컬럼이 존재하지 않습니다. 파일을 확인하세요.")

# ✅ 필요한 컬럼만 선택
category_score = category_score[["date", "category_scores"]].copy()

# ✅ `date` 컬럼을 날짜 형식으로 변환
category_score["date"] = pd.to_datetime(category_score["date"])

# ✅ `date`를 기준으로 병합
df_merged = df_merged.merge(category_score, on="date", how="left")

# ✅ 병합 후 `category_scores` 컬럼을 개별 컬럼으로 변환
def parse_category_scores(value):
    try:
        return ast.literal_eval(value) if isinstance(value, str) else value
    except Exception as e:
        print(f"⚠️ 변환 실패: {value}, 오류: {e}")
        return {}

df_merged["category_scores"] = df_merged["category_scores"].apply(parse_category_scores)

# ✅ 개별 컬럼으로 확장
category_columns = df_merged["category_scores"].apply(pd.Series)

# ✅ `category_scores` 컬럼 삭제 후 새로운 컬럼 추가
df_merged = pd.concat([df_merged.drop(columns=["category_scores"]), category_columns], axis=1)

# ✅ 변환 결과 확인
print("✅ 변환 완료! 상위 데이터 확인:")
print(df_merged.head())

# ✅ 병합된 CSV 파일 저장 (Finance_data 폴더에 저장)
df_merged.to_csv(output_path, index=False, encoding="utf-8-sig")

print(f"✅ 병합된 데이터 저장 완료: {output_path}")
