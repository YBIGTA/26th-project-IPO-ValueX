import json
from fastapi import APIRouter, HTTPException, Response, status
from Database.mongodb_connection import mongo_db
from LLM_modeling.vectorize.article_summarize import load_model, NewsTokenizer

router = APIRouter(
    prefix="/summary",
    tags=["summary"]
)

@router.post("/summarize/data")
def summarize_and_vectorize_news():
    doc_collection = mongo_db.preprocessed_news
    summary_collection = mongo_db.summary_and_vectors

    path_existence = doc_collection.find_one()
    if not path_existence:
        raise HTTPException(
            status_code=404,
            detail=f"No news document found"
        )

    news_tokenizer = NewsTokenizer(
        peft_model_dir="LLM_modeling/finetuning/mt5_large_peft_final",
        dataset_file="dummy",
        output_file="LLM_modeling/backup/summary_backup.json"
    )

    batch_size = 50
    batch_records = []
    processed_count = 0
    error_count = 0

    for doc in doc_collection.find():
        if summary_collection.find_one({"_id": doc["_id"]}):
            continue

        try:
            entity = {
                "id": str(doc["_id"]),
                "article": doc.get("Body_processed", "")
            }
            result = news_tokenizer.summarize_and_tokenize(entity)
            result["_id"] = doc["_id"]

            if "Link" in doc:
                result["Link"] = doc["Link"]

            batch_records.append(result)
            processed_count += 1

            if len(batch_records) >= batch_size:
                summary_collection.insert_many(batch_records)
                with open(news_tokenizer.output_file, "a", encoding="utf-8") as f:
                    json.dump(batch_records, f, ensure_ascii=False, indent=4)
                    f.write("\n")
                batch_records = []
        except Exception as e:
            print(f"Error processing document with _id {doc['_id']}: {e}")
            error_count += 1
            continue

    if batch_records:
        summary_collection.insert_many(batch_records)
        with open(news_tokenizer.output_file, "a", encoding="utf-8") as f:
            json.dump(batch_records, f, ensure_ascii=False, indent=4)
            f.write("\n")
    
    return {
        "message": "Summarization and vectorization completed",
        "processed": processed_count,
        "errors": error_count
    }