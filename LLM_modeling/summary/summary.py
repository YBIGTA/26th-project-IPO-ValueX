import openai
import os
from dotenv import load_dotenv
from fastapi import HTTPException

load_dotenv()

openai.api_key = os.getenv('OPENAI_API_KEY')

# ChatCompletion 클래스를 직접 import하여 사용합니다.
from openai import ChatCompletion

def generate_summary(body_text: str) -> dict:
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
                        "요약문을 작성할 때, 모든 문장의 마지막 단어는 반드시 '하다' 형태로 끝나야 합니다. 예를 들어, '정부는 새로운 정책을 발표하다. 전문가들은 이 정책이 경제에 긍정적인 영향을 미치다.'와 같이 작성해 주세요."
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