from fastapi import APIRouter
from fastapi.templating import Jinja2Templates
from starlette.requests import Request

router = APIRouter()
templates = Jinja2Templates(directory="App/templates")

@router.get("/mainpage")
async def main_page(request: Request):
    return templates.TemplateResponse("mainpage.html", {"request": request})

@router.get("/get_value")
async def get_value_page(request: Request):
    return templates.TemplateResponse("get_value.html", {"request": request})

@router.get("/suggest_news")
async def suggest_news_page(request: Request):
    return templates.TemplateResponse("suggest_news.html", {"request": request})

@router.get("/community")
async def community_page(request: Request):
    return templates.TemplateResponse("community.html", {"request": request})

@router.get("/write_community")
async def write_community_page(request: Request):
    return templates.TemplateResponse("write_community.html", {"request": request})