import json


# 파일 로드
with open("./Finance_data/IPOSTOCK_data.json", "r", encoding="utf-8") as f:
    ipostock_data = json.load(f)

with open("./Finance_data/KIND_final.json", "r", encoding="utf-8") as f:
    kind_data = json.load(f)

# KIND_data에서 기업명과 "(공모가_종가)등락률" 값을 추출
kind_dict = {item["기업명"]: item["(공모가_종가)등락률"] for item in kind_data}

# IPOSTOCK_data.json에서 해당 기업명을 찾아 "종가대비등락율"을 업데이트
for company_data in ipostock_data:
    company_name = list(company_data.keys())[0]  # 기업명 추출
    if company_name in kind_dict:
        company_data[company_name]["종가대비등락율"] = kind_dict[company_name]

# 업데이트된 데이터 저장
output_path = "./Finance_data/IPOSTOCK_data.json"
with open(output_path, "w", encoding="utf-8") as f:
    json.dump(ipostock_data, f, ensure_ascii=False, indent=4)

print(f"업데이트된 파일 저장 완료: {output_path}")
