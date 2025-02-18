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

    batch_size = 3
    batch_records = []
    processed_count = 0
    error_count = 0

    def convert_oid(record):
        rec = record.copy()
        rec["_id"] = str(rec["_id"])
        return rec

    for doc in doc_collection.find():
        if summary_collection.find_one({"_id": doc["_id"]}):
            continue

        try:
            article = doc.get("Body_processed", "")
            if not isinstance(article, str) or not article.strip():
                print(f"Invalid or empty 'Body_processed' in document with _id {doc['_id']}. Skipping.")
                continue

            entity = {
                "id": str(doc["_id"]),
                "article": article
            }
            result = news_tokenizer.summarize_and_tokenize(entity)
            result["_id"] = doc["_id"]

            if "Link" in doc:
                result["Link"] = doc["Link"]

            batch_records.append(result)
            processed_count += 1

            if len(batch_records) >= batch_size:
                for record in batch_records:
                    summary_collection.update_one(
                        {"_id": record["_id"]},
                        {"$set": record},
                        upsert=True
                    )
                with open(news_tokenizer.output_file, "a", encoding="utf-8") as f:
                    backup_records = [convert_oid(rec) for rec in batch_records]
                    json.dump(backup_records, f, ensure_ascii=False, indent=4)
                    f.write("\n")
                batch_records = []

        except Exception as e:
            err = str(e).split(":")[0]
            print(f"Error processing document with _id {doc['_id']} - {err}")
            error_count += 1
            continue

    if batch_records:
        for record in batch_records:
            summary_collection.update_one(
                {"_id": record["_id"]},
                {"$set": record},
                upsert=True
            )
        with open(news_tokenizer.output_file, "a", encoding="utf-8") as f:
            backup_records = [convert_oid(rec) for rec in batch_records]
            json.dump(backup_records, f, ensure_ascii=False, indent=4)
            f.write("\n")

    return {
        "message": "Summarization and vectorization completed",
        "processed": processed_count,
        "errors": error_count
    }