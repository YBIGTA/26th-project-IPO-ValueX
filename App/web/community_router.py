from fastapi import APIRouter, HTTPException
from pymongo import MongoClient
from datetime import datetime
import os
from dotenv import load_dotenv
from bson.objectid import ObjectId

load_dotenv()

# ✅ MongoDB 연결 설정
mongo_uri = os.getenv("MONGODB_URI")
mongo_client = MongoClient(mongo_uri, tls=True, tlsAllowInvalidCertificates=True)
mongo_db = mongo_client["Project_IPO_ValueX"]
community_collection = mongo_db["community_db"]

router = APIRouter(prefix="/api/community", tags=["community"])


### ✅ 1. 커뮤니티 게시글 전체 목록 조회
@router.get("")
def get_community_posts():
    """ 커뮤니티 게시글 전체 목록 조회 """
    posts = list(community_collection.find({}, {"_id": 1, "종목": 1, "제목": 1, "내용": 1, "글쓴이": 1, "날짜":1, "조회수": 1, "추천수": 1}))

    # ✅ `_id`를 문자열로 변환하여 반환
    for post in posts:
        post["_id"] = str(post["_id"])

    return {"status": "success", "data": posts}


### ✅ 2. 커뮤니티 게시글 작성
@router.post("")
def create_community_post(post: dict):
    """ 커뮤니티 새 게시글 추가 """
    post["조회수"] = 0
    post["추천수"] = 0
    post["날짜"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # ✅ 초까지 저장

    # ✅ 내용 추가 (DB에 저장)
    if "내용" not in post:
        post["내용"] = ""

    inserted_post = community_collection.insert_one(post)
    return {
        "status": "success",
        "message": "게시글이 등록되었습니다.",
        "id": str(inserted_post.inserted_id),
    }


### ✅ 3. 특정 게시글 조회 (내용 포함)
@router.get("/{post_id}")
def get_community_post(post_id: str):
    """ 특정 게시글 조회 (ID 기반) """
    post = community_collection.find_one({"_id": ObjectId(post_id)}, {"_id": 1, "종목": 1, "제목": 1, "내용": 1, "글쓴이": 1, "날짜": 1, "조회수": 1, "추천수": 1})
    
    if not post:
        raise HTTPException(status_code=404, detail="게시글을 찾을 수 없습니다.")

    # ✅ `_id`를 문자열로 변환
    post["_id"] = str(post["_id"])
    
    return {"status": "success", "data": post}


### ✅ 4. 특정 게시글 삭제
@router.delete("/{post_id}")
def delete_community_post(post_id: str):
    """ 특정 게시글 삭제 """
    result = community_collection.delete_one({"_id": ObjectId(post_id)})

    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="게시글을 찾을 수 없습니다.")

    return {"status": "success", "message": "게시글이 삭제되었습니다."}


### ✅ 5. 특정 게시글 조회수 증가
@router.put("/{post_id}/view")
def increment_view_count(post_id: str):
    """ 특정 게시글의 조회수를 증가 """
    result = community_collection.update_one(
        {"_id": ObjectId(post_id)},
        {"$inc": {"조회수": 1}}
    )

    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="게시글을 찾을 수 없습니다.")

    return {"status": "success", "message": "조회수가 증가되었습니다."}


### ✅ 6. 특정 게시글 추천수 증가
@router.put("/{post_id}/like")
def increment_like_count(post_id: str):
    """ 특정 게시글의 추천수를 증가 """
    result = community_collection.update_one(
        {"_id": ObjectId(post_id)},
        {"$inc": {"추천수": 1}}
    )

    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="게시글을 찾을 수 없습니다.")

    return {"status": "success", "message": "추천수가 증가되었습니다."}