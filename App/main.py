import asyncio
import uvicorn
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
import httpx

from App.news.news_router import router as news_router
from App.news.summary_router import router as summary_router
from App.config import PORT

app = FastAPI()
# app.mount("/static", StaticFiles(directory="static"), name="static")
app.include_router(news_router)
app.include_router(summary_router)

async def run_preprocessing():
    await asyncio.sleep(3)
    async with httpx.AsyncClient() as client:
        url = f"https://127.0.0.1:{PORT}/preprocess/news"
        try:
            response = await client.post(url)
            print(f"Preprocessing news: {response.status_code} - {response.text}")
        except Exception as e:
                print(f"Error in preprocessing news: {repr(e)}")

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(run_preprocessing())

if __name__ == "__main__":
    uvicorn.run("App.main:app", host="0.0.0.0", port=PORT, reload=True)