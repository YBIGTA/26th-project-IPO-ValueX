import requests
import pandas as pd
import os

# API 키 설정
apikey = "VHHQPXBUVGT6GL0SS90W"  # 발급받은 API 키를 입력하세요

# 모든 지표에 대한 설정 (최종 구조 반영)
indicators = {
    "기준금리": {"stat_code": "722Y001", "cycle": "M", "item_code1": "0101000", "item_code2": None},
    "국고채 금리 (3년)": {"stat_code": "721Y001", "cycle": "M", "item_code1": "5020000", "item_code2": None},
    "국고채 금리 (10년)": {"stat_code": "721Y001", "cycle": "M", "item_code1": "5050000", "item_code2": None},
    "원달러 환율": {"stat_code": "731Y004", "cycle": "M", "item_code1": "0000001", "item_code2": "0000100"},
    "소비자물가 상승률 (CPI)": {"stat_code": "901Y009", "cycle": "M", "item_code1": "0", "item_code2": None},
    "생산자물가 상승률 (PPI)": {"stat_code": "404Y014", "cycle": "M", "item_code1": "*AA", "item_code2": None},
    "통화량 (M2)": {"stat_code": "101Y003", "cycle": "M", "item_code1": "BBHS00", "item_code2": None},
    "무역수지": {"stat_code": "301Y017", "cycle": "M", "item_code1": "SA000", "item_code2": None},
    "기업경기실사지수(BSI)": {"stat_code": "512Y014", "cycle": "M", "item_code1": "99988", "item_code2": "BA"},
    "소비심리지수(CSI)": {"stat_code": "511Y002", "cycle": "M", "item_code1": "FME", "item_code2": "99988"},
    "산업생산지수(IPI)": {"stat_code": "901Y033", "cycle": "M", "item_code1": "A00", "item_code2": None},
    "외환보유액": {"stat_code": "732Y001", "cycle": "M", "item_code1": "99", "item_code2": None},
    "수출입 물량지수": {"stat_code": "301Y017", "cycle": "M", "item_code1": "SA000", "item_code2": None},
    "금리 스프레드 (국고채 10년 - 3년)": {"stat_code": "721Y001", "cycle": "M", "item_code1": "5020000", "item_code2": "5050000"},
}

# 사용자가 크롤링할 지표 선택
print("크롤링할 수 있는 지표 목록:")
for key in indicators.keys():
    print(f"- {key}")
indicator_name = input("크롤링할 지표를 입력하세요: ")

if indicator_name not in indicators:
    print("지원하지 않는 지표입니다. 프로그램을 종료합니다.")
    exit()

# 지표 설정 가져오기
stat_code = indicators[indicator_name]["stat_code"]
cycle = indicators[indicator_name]["cycle"]
item_code1 = indicators[indicator_name]["item_code1"]
item_code2 = indicators[indicator_name]["item_code2"]

# 날짜 입력에 따른 포맷 설정
if cycle == "D":  # 일별 데이터
    start_date = input("시작 연도를 입력하세요 (예: 20140101): ")
    end_date = input("종료 연도를 입력하세요 (예: 20250101): ")
else:  # 월별 또는 연도별 데이터
    start_date = input("시작 연도를 입력하세요 (예: 201401): ")
    end_date = input("종료 연도를 입력하세요 (예: 202501): ")

# API 요청 URL 생성 (다층적 구조 반영)
if item_code2:
    initial_url = f"https://ecos.bok.or.kr/api/StatisticSearch/{apikey}/json/kr/1/100/{stat_code}/{cycle}/{start_date}/{end_date}/{item_code1}/{item_code2}"
else:
    initial_url = f"https://ecos.bok.or.kr/api/StatisticSearch/{apikey}/json/kr/1/100/{stat_code}/{cycle}/{start_date}/{end_date}/{item_code1}"

print(f"\n[디버깅용] API 요청 URL:\n{initial_url}\n")

response = requests.get(initial_url)
result = response.json()

# 전체 데이터 개수 확인
if "StatisticSearch" not in result:
    print("API 응답에 문제가 있습니다. 입력된 지표나 기간을 확인하세요.")
    print(f"응답 메시지: {result}")  # 디버깅을 위해 응답 전체를 출력합니다.
    exit()

list_total_count = int(result['StatisticSearch']['list_total_count'])
list_count = (list_total_count // 100) + 1  # 한 번에 100개씩 페이지네이션

print(f"총 데이터 수: {list_total_count}, 페이지 수: {list_count}")

# 모든 페이지의 데이터를 수집
rows = []
for i in range(list_count):
    start = str(i * 100 + 1)
    end = str((i + 1) * 100)
    
    if item_code2:
        url = f"https://ecos.bok.or.kr/api/StatisticSearch/{apikey}/json/kr/{start}/{end}/{stat_code}/{cycle}/{start_date}/{end_date}/{item_code1}/{item_code2}"
    else:
        url = f"https://ecos.bok.or.kr/api/StatisticSearch/{apikey}/json/kr/{start}/{end}/{stat_code}/{cycle}/{start_date}/{end_date}/{item_code1}"

    response = requests.get(url)
    result = response.json()
    
    # 각 페이지의 데이터 추가
    rows.extend(result['StatisticSearch']['row'])
    print(f"페이지 {i + 1}/{list_count} 크롤링 완료")

# 데이터프레임으로 변환
df = pd.DataFrame(rows)

# 열 이름 변경 및 필요한 열만 선택
df = df[['TIME', 'DATA_VALUE']]
df.rename(columns={'TIME': '날짜', 'DATA_VALUE': indicator_name}, inplace=True)

# 저장 경로 설정
base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
output_directory = os.path.join(base_dir, "Finance_data")
os.makedirs(output_directory, exist_ok=True)
output_file = os.path.join(output_directory, f"KOREA_{indicator_name.replace(' ', '_')}.json")

# JSON 파일로 저장
df.to_json(output_file, orient="records", force_ascii=False, indent=4)
print(f"{indicator_name}의 데이터가 {output_file}에 저장되었습니다.")











