import pandas as pd
from datetime import datetime
from pykrx import stock
import os
import json

# 시작 및 종료 날짜 설정
start_date = "20140102"  # 2014년 1월 2일
end_date = datetime.now().strftime("%Y%m%d")  # 오늘 날짜

# Finance_data 디렉토리 설정
output_directory = os.path.join(os.path.dirname(os.path.dirname(__file__)), "Finance_data")
os.makedirs(output_directory, exist_ok=True)

def get_index_data_by_code(sector_code):
    print(f"지수 코드 {sector_code} 데이터 크롤링 중...")
    
    # 날짜, 종가(Close), 거래량(Volume) 데이터만 수집
    sector_data = stock.get_index_ohlcv_by_date(start_date, end_date, sector_code)[['시가','종가', '거래량']]
    sector_data.reset_index(inplace=True)  # 날짜를 index에서 컬럼으로 변환
    sector_data['날짜'] = sector_data['날짜'].apply(lambda x: x.strftime("%Y-%m-%d"))  # 사람이 읽을 수 있는 날짜로 변환
    
    return sector_data

# 사용자 입력: 크롤링할 지수 코드 입력
sector_code = input("크롤링할 지수 코드를 입력하세요 (예: 1013 for IT·반도체): ")

# 데이터 수집
index_data = get_index_data_by_code(sector_code)

# 파일 저장 경로 설정
output_file = os.path.join(output_directory, f"KOSPI_{sector_code}.json")

# JSON으로 저장
index_data.to_json(output_file, orient="records", force_ascii=False, indent=4)
print(f"지수 코드 {sector_code}의 데이터가 {output_file}에 저장되었습니다.")




