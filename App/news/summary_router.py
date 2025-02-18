from fastapi import APIRouter, HTTPException, Response, status
from Database.mongodb_connection import mongo_db
from LLM_modeling.vectorize.article_summarize import load_model, NewsTokenizer

router = APIRouter(
    prefix="/summary",
    tags=["summary"]
)

@router.post("/summarize/data")
def summarize_and_vectorize_news():
    #TODO
    '''
    raw news: 로컬에서 받아오기
    preprocessed news: MongoDB에 올리기

    필요한 함수: 프로세싱 함수
    '''
    pass
    