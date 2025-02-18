from fastapi import APIRouter, HTTPException, Response, status
from Database.mongodb_connection import mongo_db

router = APIRouter(
    prefix="/news",
    tags=["news"]
    )

@router.post("/preprocess/news")
def preprocess_news():
    pass
    #TODO

