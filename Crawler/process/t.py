import json

with open("./Finance_data/IPOSTOCK_data.json", "r", encoding="utf-8") as f:
    ipostock_data = json.load(f)

# 특정 키의 개행 및 탭 문자 제거
for company_data in ipostock_data:
    company_name = list(company_data.keys())[0]  # 기업명 추출
    
    주주구성 = company_data[company_name].get("주주구성", {})
    주주구성_table = 주주구성.get("주주구성 table", {})
    
    if isinstance(주주구성_table, dict):
        for section in 주주구성_table:
            if isinstance(주주구성_table[section], dict):
                for key in list(주주구성_table[section].keys()):
                    new_key = key.replace("\n", "").replace("\t", "")
                    if new_key != key:
                        주주구성_table[section][new_key] = 주주구성_table[section].pop(key)

# 업데이트된 데이터 저장
output_path = "./Finance_data/IPOSTOCK_data.json"
with open(output_path, "w", encoding="utf-8") as f:
    json.dump(ipostock_data, f, ensure_ascii=False, indent=4)

print(f"업데이트된 파일 저장 완료: {output_path}")
