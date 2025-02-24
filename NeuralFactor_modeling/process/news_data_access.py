from pymongo import MongoClient
from dotenv import load_dotenv
import pandas as pd
import os
import sys

# ✅ `.env` 파일 로드
load_dotenv()
mongo_uri = os.getenv("MONGODB_URI")

# ✅ MongoDB URI 확인
if not mongo_uri:
    print("❌ `.env`에서 `MONGODB_URI`를 가져오지 못했습니다.")
    sys.exit(1)

# ✅ MongoDB 연결
try:
    client = MongoClient(mongo_uri, tls=True, tlsAllowInvalidCertificates=True)
    db = client["Project_IPO_ValueX"]
    collection = db["category_score"]
except Exception as e:
    print(f"❌ MongoDB 연결 실패: {e}")
    sys.exit(1)

# ✅ 컬렉션에서 데이터 가져오기
data = list(collection.find())

if not data:
    print("⚠️ `category_score` 컬렉션에 데이터가 없습니다.")
    sys.exit(1)

# ✅ `_id` 필드 제거
for doc in data:
    doc.pop("_id", None)

# ✅ DataFrame 변환
df = pd.DataFrame(data)

# ✅ 저장할 디렉토리 설정 (`Desktop/26th-project-IPO-ValueX/NeuralFactor_modeling/`)
base_dir = os.path.join(os.path.expanduser("~"), "Desktop", "26th-project-IPO-ValueX", "NeuralFactor_modeling")

# ✅ 디렉토리 생성 (없으면 생성)
os.makedirs(base_dir, exist_ok=True)
print(f"📁 저장할 디렉토리 확인: {base_dir}")
print(f"📁 디렉토리 존재 여부: {os.path.exists(base_dir)}")

# ✅ CSV 파일 저장 경로 설정
csv_file_path = os.path.join(base_dir, "category_score.csv")

# ✅ CSV 저장 실행
df.to_csv(csv_file_path, index=False, encoding="utf-8-sig")

# ✅ 파일 저장 여부 확인
if os.path.exists(csv_file_path):
    print(f"✅ CSV 파일이 정상적으로 생성되었습니다: {csv_file_path}")
else:
    print(f"❌ CSV 파일이 생성되지 않았습니다. 경로를 확인하세요: {csv_file_path}")
