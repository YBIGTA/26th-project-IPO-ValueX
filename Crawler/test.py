import requests
import pandas as pd

# API 키 설정
api_key = "VHHQPXBUVGT6GL0SS90W"  # 발급받은 API 키를 입력하세요

# 통계표 코드 설정 (예: 기준금리)
stat_code = "722Y001"  # 확인하려는 지표의 통계표 코드를 입력하세요

# API 요청 URL 생성
url = f"https://ecos.bok.or.kr/api/StatisticItemList/{api_key}/json/kr/1/100/{stat_code}"

# API 요청 보내기
response = requests.get(url)

# 응답 데이터 처리
if response.status_code == 200:
    data = response.json().get("StatisticItemList", {}).get("row", [])
    if not data:
        print("데이터가 없습니다. 통계표 코드를 확인하세요.")
    else:
        # 데이터프레임으로 변환
        df = pd.DataFrame(data)
        
        # 필요한 열 선택 및 이름 변경
        df = df[['ITEM_NAME', 'START_TIME', 'END_TIME']]
        df.rename(columns={'ITEM_NAME': '항목명', 'START_TIME': '수집 시작일자', 'END_TIME': '수집 종료일자'}, inplace=True)
        
        # 결과 출력
        print(df)
else:
    print("API 요청 실패:", response.status_code)


