import asyncio
import uvicorn
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
import httpx

from App.news.news_router import router as news_router
from App.news.news_processed_router import router as summary_router
from App.finance.finance_router import router as finance_router
from App.finance.finance_processed_router import router as finance_processed_router
from App.config import PORT

app = FastAPI()
app.include_router(news_router)
app.include_router(summary_router)
app.include_router(finance_router)
app.include_router(finance_processed_router)

async def make_request(url: str, retry: int = 3):
    """ 🔄 API 호출을 수행하고 실패하면 재시도하는 함수 """
    async with httpx.AsyncClient() as client:
        for attempt in range(retry):
            try:
                response = await client.post(url)
                print(f"✅ Request {url}: {response.status_code} - {response.text}")
                return response
            except Exception as e:
                print(f"❌ Error in request {url} (Attempt {attempt+1}/{retry}): {repr(e)}")
                await asyncio.sleep(3)  # 실패 시 3초 대기 후 재시도
    print(f"🚨 Failed to complete request {url} after {retry} attempts.")

# 📌 뉴스 & 요약 관련 API 
@app.post("/run/news/preprocessing")
async def run_news_preprocessing(years: str = ""):
    """ 📰 뉴스 데이터 전처리 수동 실행 (연도 필터 적용 가능, 기본값: 전체 연도) """
    
    # `years`가 지정되지 않았으면 전체 처리 (쿼리 파라미터 제거)
    if years:
        url_news = f"http://127.0.0.1:{PORT}/news/preprocess/news?mode=local&years={years}"
        message = f"✅ News preprocessing started for years: {years}"
    else:
        url_news = f"http://127.0.0.1:{PORT}/news/preprocess/news?mode=local"
        message = "✅ News preprocessing started for all years"

    await make_request(url_news)
    return {"message": message}

@app.post("/run/news/summarization")
async def run_news_summarization():
    """ 📰 뉴스 데이터 요약 및 벡터화 수동 실행 (기본: db 모드) """
    url_summary = f"http://127.0.0.1:{PORT}/summary/summarize/data?mode=db"
    await make_request(url_summary)
    return {"message": "✅ News summarization started"}

# 📌 금융 데이터 관련 API
@app.post("/run/finance/preprocessing")
async def run_finance_preprocessing():
    """ 📊 금융 데이터 전처리 수동 실행 """
    url_finance = f"http://127.0.0.1:{PORT}/finance/preprocess"
    await make_request(url_finance)
    return {"message": "✅ Finance preprocessing started"}

@app.post("/run/finance/processing")
async def run_finance_processing():
    """ 📊 금융 데이터 가공 후 저장 (IPOSTOCK, Finance_by_month, Finance_by_date → finance_processed) """
    url_finance_processing = f"http://127.0.0.1:{PORT}/finance/processed/process"
    await make_request(url_finance_processing)
    return {"message": "✅ Finance data processing started"}

# @app.on_event("startup")
# async def startup_event():
#     """ 🚀 서버 시작 시 자동으로 뉴스 및 금융 데이터 전처리 및 요약 실행 (현재 주석 처리) """
#     asyncio.create_task(run_news_preprocessing())
#     asyncio.create_task(run_news_summarization())
#     asyncio.create_task(run_finance_preprocessing())
#     asyncio.create_task(run_finance_processing())

if __name__ == "__main__":
    uvicorn.run("App.main:app", host="0.0.0.0", port=PORT, reload=True)