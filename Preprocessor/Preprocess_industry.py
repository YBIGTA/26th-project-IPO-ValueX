import os
import json
import pandas as pd

# 📌 Base directory 설정
base_directory = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# 📌 Finance_data 디렉토리 경로 설정
finance_data_directory = os.path.join(base_directory, "Finance_data")

# 📌 파일 경로 설정
json_file_path = os.path.join(finance_data_directory, "KIND_Final.json")  # JSON 파일 위치
csv_file_path = os.path.join(finance_data_directory, "기업명_상장일_산업군.csv")  # 기존 산업군 데이터 위치
output_file_path = os.path.join(finance_data_directory, "2nd기업명_상장일_산업군.csv")  # 최종 저장 파일

# 📌 JSON 파일 로드
try:
    with open(json_file_path, "r", encoding="utf-8") as file:
        data = json.load(file)
    df = pd.DataFrame(data)
    print("✅ JSON 파일 로드 완료")
except FileNotFoundError:
    raise FileNotFoundError(f"❌ JSON 파일을 찾을 수 없습니다: {json_file_path}")

# 📌 기존 산업군 CSV 파일 로드
try:
    existing_df = pd.read_csv(csv_file_path, encoding="utf-8-sig", header=0)
    print("✅ CSV 파일 로드 완료")
except FileNotFoundError:
    raise FileNotFoundError(f"❌ CSV 파일을 찾을 수 없습니다: {csv_file_path}")
except UnicodeDecodeError:
    existing_df = pd.read_csv(csv_file_path, encoding="cp949", header=0)
    print("✅ CSV 파일을 CP949 인코딩으로 로드 완료")

# 📌 컬럼명 확인 및 정리
expected_columns = {'기업명', '상장일'}

if not expected_columns.issubset(df.columns):
    raise KeyError(f"❌ JSON 파일에서 필요한 컬럼이 없습니다. 확인하세요: {df.columns.tolist()}")

if not expected_columns.issubset(existing_df.columns):
    raise KeyError(f"❌ CSV 파일에서 필요한 컬럼이 없습니다. 확인하세요: {existing_df.columns.tolist()}")

# 📌 날짜 변환 및 정렬 (최신 날짜가 위로)
df['상장일'] = pd.to_datetime(df['상장일'], errors='coerce')
existing_df['상장일'] = pd.to_datetime(existing_df['상장일'], errors='coerce')

df = df.sort_values(by='상장일', ascending=False)

# 📌 병합: 기존 산업군 값 유지, 새로운 데이터는 공란
merged_df = pd.merge(df, existing_df, on=['기업명', '상장일'], how='left')

# 📌 산업군이 없는 경우 빈칸 처리
if '산업군' in merged_df.columns:
    merged_df['산업군'] = merged_df['산업군'].fillna("")

# 📌 CSV 파일로 저장
merged_df.to_csv(output_file_path, index=False, encoding="utf-8-sig")

print(f"✅ 데이터가 성공적으로 저장되었습니다: {output_file_path}")

