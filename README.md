# 26th-project-IPO-ValueX
26기 신입기수 프로젝트 - 산업군별 최적화 IPO 밸류에이션

1. 패키지 설치
pip install -r requirements.txt
mac의 경우 : bash install_mecab.sh
2. env 파일 설정 & 연결 확인
python Database/mongodb_connection.py      
3. api 실행
PYTHONPATH=$(pwd) #설정 해도 되고 안해도 되고 본인에 맞게
- uvicorn App.main:app --reload --workers=1 : 이걸로 실행하시오.
- uvicorn App.main:app --host 0.0.0.0 --port 8000 --reload: 자동처리 가능
4. 다른 터미널에서 news 관련 Post 요청
4-1-1. 텍스트 전처리 실행(로컬 모드)
curl -X 'POST' 'http://127.0.0.1:8000/news/preprocess/news?mode=local' -H 'accept: application/json' 
- 모드 뒤에 '&years=2018,2019' 붙이면 해당 년도만 저장 가능
curl -X 'POST' 'http://127.0.0.1:8000/news/preprocess/news?mode=local&years=2025,2021' -H 'accept: application/json'
4-1-2. 텍스트 전처리 실행(크롤러 모드)
curl -X 'POST' 'http://127.0.0.1:8000/news/preprocess/news?mode=crawler' -H 'accept: application/json'
4-2-1. 요약 및 벡터화 실행(로컬 다운 모드)
curl -X 'POST' 'http://127.0.0.1:8000/summary/summarize/data?mode=local' -H 'accept: application/json'
4-2-2. 요약 및 벡터화 실행(db 다운 모드)
curl -X 'POST' 'http://127.0.0.1:8000/summary/summarize/data?mode=db' -H 'accept: application/json'
5. 다른 터미널에서 finance 관련 Post 요청
5-1-1. 크롤링된 재무 데이터 업로드 실행(CSV 데이터를 기반으로 업로드)
curl -X POST "http://127.0.0.1:8000/finance/upload"
5-1-2. 크롤링된 재무 데이터 전처리 후 업로드 실행(JSON 데이터를 기반으로 업로드)
curl -X POST "http://127.0.0.1:8000/finance/upload?load_from_json=true"
5-2. 재무 데이터 처리 (1단계: 날짜 기반 병합)
curl -X POST "http://127.0.0.1:8000/finance/merged/process_date"
5-3. 재무 데이터 처리 (2단계: 최종 데이터 병합)
curl -X POST "http://127.0.0.1:8000/finance/merged/process_final"
6. 다른 터미널에서 38 관련 Post 요청
6-1. 크롤링된 raw 데이터 업로드
curl -X 'POST' 'http://127.0.0.1:8000/community_38/upload'
6-2-1. 데이터 전처리 for KOTE(local 모드)
curl -X 'POST' 'http://127.0.0.1:8000/community_38/processed?mode=local'
6-2-2. 데이터 전처리 for KOTE(db 모드)
curl -X 'POST' 'http://127.0.0.1:8000/community_38/processed?mode=db'
6-3. 데이터 처리 for 회귀
curl -X 'POST' 'http://127.0.0.1:8000/community_38/postprocess_regression'
6-4. 데이터 처리 for 뉴럴팩터
curl -X 'POST' 'http://127.0.0.1:8000/community_38/postprocess_neural'


