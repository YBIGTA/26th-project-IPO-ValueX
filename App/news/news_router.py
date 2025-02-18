from fastapi import APIRouter, HTTPException, Response, status
from Database.mongodb_connection import mongo_db

import os

router = APIRouter(
    prefix="/news",
    tags=["news"]
    )

@router.post("/preprocess/news")
def preprocess_news():
    raw_path = os.path.join(os.getcwd(), "Non_Finance_data", "Naver_Stock")
    category_path = os.path.join(os.getcwd(), "Database", "sector_vocab")
    files = [os.path.join(raw_path, file) for file in os.listdir(raw_path)]
    category_files = {file.split('.')[0]: os.path.join(category_path, file) for file in os.listdir(category_path)}

    preprocessed_news_collection = mongo_db.preprocessed_news

    if not files:
        raise HTTPException(
            status_code=404,
            detail=f"No raw news found for your local directory '{raw_path}'"
        )
    
    import pandas as pd
    from Preprocessor_NFdata.Preprocess_news import run_preprocess_naver
    from Preprocessor_NFdata.Preprocess_tfidf import run_semi_tfidf
    from Preprocessor_NFdata.Preprocess_tfidf import load_category_vocab

    batch = 3000

    for file in files:
        df = pd.read_csv(file, encoding='utf-8')
        cnt = 0
        n = 0
        while cnt * batch < len(df):
            try:
                d = df.iloc[cnt * batch: min((cnt+1) * batch, len(df))]
                _ = run_preprocess_naver(d)
                processed_news = run_semi_tfidf(_, category_files)
                if processed_news is not None:
                    if isinstance(processed_news, tuple):
                        processed_news = processed_news[0]
                    records = processed_news.to_dict('records')
                    preprocessed_news_collection.insert_many(records)

                    n += len(processed_news)
                cnt += 1
            except:
                print(f"error occured in {cnt+1}th batch processing")
                cnt += 1

        print(f"Year: {file.split('.')[-2][-4:]} data saved to database. {len(df)} -> {n}")
        
    return {"message": "All data saved"}
