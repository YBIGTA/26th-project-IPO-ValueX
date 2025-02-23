import openai
from dotenv import load_dotenv
from fastapi import HTTPException

load_dotenv()

def generate_summary_and_keywords(body_text: str) -> dict:
    try:
        summary_response = openai.ChatCompletion.create(
            model="gpt-4",
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
                    )
                }
            ],
            temperature=0.2,
            top_p=0.5,
            max_tokens=200,
        )
        summary = summary_response.choices[0].message.content.strip()
        
        keywords_response = openai.ChatCompletion.create(
            model="gpt-4",
            messages=[
                {
                    "role": "system",
                    "content": (
                        "You are an AI language model specialized in extracting key information from news articles. "
                        "Please extract 2 important nouns as keywords."
                    )
                },
                {
                    "role": "user",
                    "content": (
                        f"뉴스 기사 본문:\n\n{body_text}\n\n"
                        f"뉴스 요약본:\n\n{summary}\n\n "
                        "위 기사와 요약본을 참고하여 2개의 핵심 키워드(명사)를 추출해 주세요. "
                        "키워드는 명사만 포함하고, 쉼표로 구분해 주세요."
                    )
                }
            ],
            temperature=0.2,
            top_p=0.5,
            max_tokens=100,
        )
        keywords = keywords_response.choices[0].message.content.strip()
        
        return {"summary": summary, "keywords": keywords}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error during summary and keywords generation: {e}")



def generate_rag_response(summary: str, retrieved_docs: list, market_indicators=None) -> str:
    """
    제공된 context(여러 뉴스 요약 및 키워드 결과)를 바탕으로 GPT-4가 추가적인 추론을 진행하도록 합니다.
    """
    system_message = {
        "role": "system",
        "content": (
            "You are GPT-4, an expert in news analysis. You use chain-of-thought reasoning to provide detailed answers. "
            "Please carefully review the provided context and answer the query with detailed, step-by-step reasoning in Korean."
        )
    }
    context_message = {
        "role": "system",
        "content": "Retrieved Documents:\n" + "\n =============== \n".join(retrieved_docs) + "\n\n"
    }
    user_message = {
        "role": "user",
        "content": (
            f"주어진 정보(Retrieved Documents)를 바탕으로, 요약본과 내용이 유사한 문서를 최소 2개 이상 선택하세요:\n{summary}\n\n"
            "그리고 선택한 문서들을 바탕으로, 위 요약본에 대한 일반적인 추가 설명을 단계별 추론과 함께 제공해 주세요."
        )
    }
    try:
        response = openai.ChatCompletion.create(
            model="gpt-4",
            messages=[system_message, context_message, user_message],
            temperature=0.5,
            max_tokens=1024,
            top_p=0.9,
            frequency_penalty=0,
            presence_penalty=0
        )  
        return response.choices[0].message.content
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error during RAG generation: {e}")

