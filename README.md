# 26th-project-IPO-ValueX
26기 신입기수 프로젝트 - 산업군별 최적화 IPO 밸류에이션

1. 패키지 설치
pip install -r requirements.txt
mac의 경우 : bash install_mecab.sh
2. env 파일 설정 & 연결 확인
python Database/mongodb_connection.py      
3. api 실행
PYTHONPATH=$(pwd) #설정 해도 되고 안해도 되고 본인에 맞게
uvicorn App.main:app --reload --workers=1
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
5-1. 크롤링된 재무 데이터 업로드 실행(로컬 모드 - 다른 모드 안 만듦)
curl -X POST "http://127.0.0.1:8000/finance/upload"
5-2. 재무 데이터 전처리해서 병합 실행(db 다운 모드- 다른 모드 안 만듦)
curl -X POST "http://127.0.0.1:8000/finance/processed/process"
