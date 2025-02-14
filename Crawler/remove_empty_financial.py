import json

# JSON 파일 로드
input_path = "./Finance_data/IPOSTOCK_data.json"
output_path = "./Finance_data/filtered_IPOSTOCK_data.json"

with open(input_path, "r", encoding="utf-8-sig") as f:
    ipostock_data = json.load(f)

# 제거된 기업 리스트 저장
removed_companies = []

# 필터링하여 "재무정보"가 빈 기업 제거
filtered_data = []
for company in ipostock_data:
    company_name = list(company.keys())[0]  # 기업명 추출
    if not company[company_name].get("재무정보"):  # "재무정보"가 없거나 빈 딕셔너리인 경우
        removed_companies.append(company_name)
    else:
        filtered_data.append(company)

# 필터링된 데이터 저장
with open(output_path, "w", encoding="utf-8-sig") as f:
    json.dump(filtered_data, f, ensure_ascii=False, indent=4)

# 결과 출력
print(f"✅ 재무정보가 없는 기업을 제거한 JSON 파일이 저장되었습니다: {output_path}")
if removed_companies:
    print("🚨 제거된 기업 목록:")
    for company in removed_companies:
        print(f"- {company}")
else:
    print("✅ 제거된 기업이 없습니다.")
