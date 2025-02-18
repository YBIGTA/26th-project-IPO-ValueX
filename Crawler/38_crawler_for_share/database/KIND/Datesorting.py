import json
from datetime import datetime

def load_json(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        return json.load(f)

def save_json(data, file_path):
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)

def sort_and_deduplicate_json(json_file, output_file):
    # JSON 로드
    data = load_json(json_file)
    
    # 중복 제거를 위한 기업명 기준으로 딕셔너리 생성 (최근 데이터만 유지)
    unique_data = {item["기업명"]: item for item in sorted(data, key=lambda x: datetime.strptime(x["상장일"], "%Y-%m-%d"), reverse=True)}
    
    # 정렬된 리스트로 변환
    sorted_unique_data = list(unique_data.values())
    
    # 결과 저장
    save_json(sorted_unique_data, output_file)
    print(f"Final sorted and deduplicated data saved to {output_file}")

# 사용 예시
sort_and_deduplicate_json("KIND_Sorted.json", "KIND_Last.json")

