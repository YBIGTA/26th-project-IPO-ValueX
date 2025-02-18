from fastapi import APIRouter, HTTPException, Response, status
from Database.mongodb_connection import mongo_db

router = APIRouter(
    prefix="/news",
    tags=["news"]
    )

@router.post("/preprocess/news")
def preprocess_news():
    #TODO
    '''
    raw news: 로컬에서 받아오기
    preprocessed news: MongoDB에 올리기

    필요한 함수: 프로세싱 함수
    '''
    pass
    
