import openai
import os
from dotenv import load_dotenv
from fastapi import HTTPException

load_dotenv()

openai.api_key = os.getenv('OPENAI_API_KEY')

# ChatCompletion 클래스를 직접 import하여 사용합니다.
from openai import ChatCompletion

def generate_summary(body_text: str) -> str:
    """
    뉴스 기사 본문(body_text)을 입력으로 받아:
    1) 2~3문장 분량의 요약(summary),
    2) 키워드 2개(keywords)
    를 생성하여 딕셔너리로 반환합니다.
    """
    try:
        # 1) 요약 생성
        summary_response = ChatCompletion.create(
            model="gpt-4o",
            messages=[
                {
                    "role": "system",
                    "content": (
                        "You are an AI language model specialized in summarizing news articles. "
                        "Please produce a concise 2 or 3-sentence summary in Korean."
                    )
                },
                {
                    "role": "user",
                    "content": (
                        f"뉴스 기사 본문:\n\n{body_text}\n\n"
                        "위 기사에 대해 2문장 또는 3문장 요약을 제공해 주세요."
                        "요약문을 작성할 때, 모든 문장은 존댓말 어조(격식체)로 해야 합니다."
                    )
                }
            ],
            temperature=0.2,
            top_p=0.5,
            max_tokens=200,
        )
        summary = summary_response.choices[0].message.content.strip()
        
        return summary
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error during summary and keywords generation: {e}")
    
def generate_tag(body_text: str) -> str:
    try:
        tag_response = ChatCompletion.create(
            model="gpt-4o",
            messages=[
                {
                    "role": "system",
                    "content": (
                        "You are an AI assistant specialized in categorizing news articles. "
                        "You will receive a news article summary and need to choose the most relevant tag "
                        "from the provided list."
                    )
                },
                {
                    "role": "user",
                    "content": (
                        f"뉴스 기사 요약문:\n\n{body_text}\n\n"
                        "아래 태그 목록 중 가장 적합한 하나의 태그를 선택해 주세요.\n"
                        "태그 목록: IT, 바이오, 게임, 배터리, 화학, 기계, 건설, 자동차, 금속, 에너지, 식품, 유통, 서비스, 금융, 증권, 보험, 반도체\n"
                        "만약 해당되는 태그가 없으면 '기타'를 출력해 주세요."
                        "만약 다양한 태그에 해당되면 '종합'을 출력해 주세요."
                    )
                }
            ],
            temperature=0.2,
            top_p=0.5,
            max_tokens=50,
        )
        tag = tag_response.choices[0].message.content.strip()
        return tag
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error during tag generation: {e}")