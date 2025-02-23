import httpx
from fastapi import APIRouter
import xml.etree.ElementTree as ET
import ast  # 안전한 문자열 → 리스트 변환용

router = APIRouter()

NAVER_KOSPI_URL = "https://api.finance.naver.com/siseJson.naver?symbol=KOSPI&requestType=1&startTime=20240222&endTime=20240222"
NAVER_KOSDAQ_URL = "https://api.finance.naver.com/siseJson.naver?symbol=KOSDAQ&requestType=1&startTime=20240222&endTime=20240222"

async def fetch_stock_data(url: str):
    async with httpx.AsyncClient() as client:
        response = await client.get(url)

        # 🔍 응답 상태 코드 & 헤더 확인
        print("🔹 응답 상태 코드:", response.status_code)
        print("🔹 응답 헤더:", response.headers)

        content_type = response.headers.get("Content-Type", "")

        # **EUC-KR 인코딩 문제 해결**
        try:
            response_text = response.content.decode("euc-kr")  # EUC-KR → UTF-8 변환
        except UnicodeDecodeError:
            print("❌ EUC-KR 디코딩 실패")
            return {"error": "EUC-KR decoding failed", "details": response.content[:500]}

        print("🔹 응답 본문 (앞부분만):", response_text[:500])

        # **XML 응답 처리**
        if "text/xml" in content_type or "application/xml" in content_type:
            if response_text.startswith("[["):
                print("⚠ XML 대신 테이블 데이터가 응답됨. 변환 진행.")
                return convert_table_to_json(response_text)
            else:
                print("❌ XML 응답이 아님.")
                return {"error": "Invalid XML response", "details": response_text[:500]}

        # **CSV 또는 리스트 형태 응답 처리**
        elif response_text.startswith("[["):
            print("⚠ 네이버 응답이 XML이 아니라 테이블 형식입니다. 변환 진행.")
            return convert_table_to_json(response_text)

        else:
            print(f"❌ 알 수 없는 응답 형식: {content_type}")
            return {"error": "Invalid response format", "details": response_text[:500]}


def convert_table_to_json(table_text: str):
    """
    네이버에서 반환한 테이블 형식 데이터를 JSON으로 변환
    """
    try:
        rows = ast.literal_eval(table_text)  # 안전하게 문자열을 리스트로 변환
        headers = rows[0]  # 첫 번째 행: 컬럼명
        data = [dict(zip(headers, row)) for row in rows[1:]]  # 데이터 매핑

        return {"status": "success", "data": data}
    except Exception as e:
        print(f"❌ 테이블 변환 실패: {e}")
        return {"error": "Table format conversion failed", "details": table_text[:500]}
        
@router.get("/api/kospi")
async def get_kospi():
    data = await fetch_stock_data(NAVER_KOSPI_URL)

    if "error" in data:
        return {"status": "error", "message": data["error"], "details": data["details"]}

    return {"status": "success", "data": data}

@router.get("/api/kosdaq")
async def get_kosdaq():
    """ 📊 코스닥 지수 가져오기 """
    data = await fetch_stock_data(NAVER_KOSDAQ_URL)

    if "error" in data:
        return {"status": "error", "message": data["error"], "details": data["details"]}
    
    return {"index": "KOSDAQ", "data": data}