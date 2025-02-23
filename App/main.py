import asyncio
import uvicorn
from fastapi import FastAPI
import httpx
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.requests import Request

from App.news.news_processed_router import router as news_router
from App.news.summary_router import router as summary_router
from App.finance.finance_processed_router import router as finance_router
from App.finance.finance_merge_processed_router import router as finance_merge_router
from App.community_38.community_38_router import router as community_38_router
from App.community_38.community_38_processed_router import router as community_38_processed_router
from App.web.web_router import router as web_router
from App.finance.krx_stock_router import router as stock_router
from App.web.ipo_list_router import router as ipo_list_router
from App.web.community_router import router as community_router
from App.news.news_summary_router import router as news_summary_router

from App.config import PORT

app = FastAPI()

# 📌 라우터 추가
app.include_router(news_router)
app.include_router(summary_router)
app.include_router(finance_router)
app.include_router(finance_merge_router)
app.include_router(community_38_router)
app.include_router(community_38_processed_router)
app.include_router(web_router)
app.include_router(stock_router) 
app.include_router(ipo_list_router)
app.include_router(community_router)
app.include_router(news_summary_router)

# 📌 정적 파일 제공 (CSS, JS 등)
app.mount("/static", StaticFiles(directory="App/static"), name="static")

# 📌 템플릿 설정 (HTML 렌더링용)
templates = Jinja2Templates(directory="App/templates")


# 📌 기본 메인 페이지 (/) → mainpage.html 렌더링
@app.get("/")
async def read_main(request: Request):
    return templates.TemplateResponse("mainpage.html", {"request": request})


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
    url_news = f"http://127.0.0.1:{PORT}/news/preprocess/news?mode=local"
    if years:
        url_news += f"&years={years}"
    await make_request(url_news)
    return {"message": f"✅ News preprocessing started for {'all years' if not years else years}"}


@app.post("/run/news/summarization")
async def run_news_summarization():
    """ 📰 뉴스 데이터 요약 및 벡터화 수동 실행 (기본: db 모드) """
    url_summary = f"http://127.0.0.1:{PORT}/summary/summarize/data?mode=db"
    await make_request(url_summary)
    return {"message": "✅ News summarization started"}


# 📌 금융 데이터 관련 API
@app.post("/run/finance/preprocessing")
async def run_finance_preprocessing():
    """ 📊 금융 데이터 업로드 (로컬 CSV → MongoDB) """
    url_finance_upload = f"http://127.0.0.1:{PORT}/finance/upload?load_from_json=false"
    await make_request(url_finance_upload)
    return {"message": "✅ Finance data upload started"}


@app.post("/run/finance/processing")
async def run_finance_processing():
    """ 📊 금융 데이터 가공 후 저장 (1단계: 날짜 병합) """
    url_finance_processing = f"http://127.0.0.1:{PORT}/finance/merged/process_date"
    await make_request(url_finance_processing)
    return {"message": "✅ Finance data processing started"}


@app.post("/run/finance/final_processing")
async def run_finance_final_processing():
    """ 📊 금융 데이터 최종 가공 후 저장 (2단계: IPOSTOCK 데이터 병합) """
    url_finance_final_processing = f"http://127.0.0.1:{PORT}/finance/merged/process_final"
    await make_request(url_finance_final_processing)
    return {"message": "✅ Finance final data processing started"}


# 📌 커뮤니티 38 데이터 관련 API
@app.post("/run/community_38/upload")
async def run_community_38_upload():
    """ 🏛 38 원본 데이터 업로드 """
    url_community_38_upload = f"http://127.0.0.1:{PORT}/community_38/upload"
    await make_request(url_community_38_upload)
    return {"message": "✅ 38 community data upload started"}


@app.post("/run/community_38/processing")
async def run_community_38_processing():
    """ 🏛 38 데이터 전처리 """
    url_community_38_processing = f"http://127.0.0.1:{PORT}/community_38/processed?mode=db"
    await make_request(url_community_38_processing)
    return {"message": "✅ 38 community data preprocessing started"}


if __name__ == "__main__":
    uvicorn.run("App.main:app", host="0.0.0.0", port=PORT, reload=True)