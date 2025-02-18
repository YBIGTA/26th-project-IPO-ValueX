import json
import pandas as pd
import os

# JSON 파일 로드
# 현재 스크립트가 위치한 preprocessor 폴더의 부모 디렉토리 (최상위 경로) 찾기
base_directory = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Finance_data 디렉토리 경로 설정
finance_data_directory = os.path.join(base_directory)

# JSON 파일이 Finance_data 폴더에 있을 경우 해당 경로 설정
json_file_path = os.path.join(finance_data_directory, "KIND\KIND_Last.json")

# JSON 파일 로드
with open(json_file_path, "r", encoding="utf-8") as file:
    data = json.load(file)

# 데이터프레임 변환
df = pd.DataFrame(data)

# 중복 기업명 제거 (첫 번째 항목만 유지)
df = df.drop_duplicates(subset=['기업명'], keep='first')

# '스팩' 포함 기업 제거
df = df[~df['기업명'].str.contains('스팩', na=False)]

# 새로운 JSON 파일로 저장
final_json_file_path = os.path.join(finance_data_directory, "KIND\KIND_38.json") 
df.to_json(final_json_file_path, orient="records", force_ascii=False, indent=4)

print(f"정제된 JSON 파일이 저장되었습니다: {final_json_file_path}")
