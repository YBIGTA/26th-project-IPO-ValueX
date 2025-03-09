import os
import json
import math
from datetime import datetime, timedelta
from transformers import AutoModelForSequenceClassification, AutoTokenizer, TextClassificationPipeline
from tqdm import tqdm
from collections import defaultdict

# ✅ 로컬 파일 경로 설정
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
INPUT_FILE = os.path.join(BASE_DIR, "38_KOTE_Input.json")
FE2_OUTPUT_FILE = os.path.join(BASE_DIR, "kf_deberta_kote_final_ver1.json")
FE2_DAILY_OUTPUT_FILE = os.path.join(BASE_DIR, "kf_deberta_kote_final_ver2.json")
KIND_FILE = os.path.join(BASE_DIR, "KIND_38.json")

# ✅ 감성 분석 모델 로드
MODEL_PATH = os.path.join(BASE_DIR, "kf_deberta_kote_model", "final_model")
model = AutoModelForSequenceClassification.from_pretrained(MODEL_PATH)
tokenizer = AutoTokenizer.from_pretrained("kakaobank/kf-deberta-base")

# ✅ 감성 분석 파이프라인 생성
pipe = TextClassificationPipeline(
    model=model,
    tokenizer=tokenizer,
    device=0,  # 🔥 GPU 사용
    top_k=None,  # ✅ 모든 감정 확률 출력
    function_to_apply='sigmoid'
)

# ✅ 감정 이름 매핑 (KOTE 감정 44개)
LABELS = ["불평/불만", "환영/호의", "감동/감탄", "지긋지긋", "고마움", "슬픔", "화남/분노",
    "존경", "기대감", "우쭐댐/무시함", "안타까움/실망", "비장함", "의심/불신", "뿌듯함",
    "편안/쾌적", "신기함/관심", "아껴주는", "부끄러움", "공포/무서움", "절망", "한심함",
    "역겨움/징그러움", "짜증", "어이없음", "없음", "패배/자기혐오", "귀찮음", "힘듦/지침",
    "즐거움/신남", "깨달음", "죄책감", "증오/혐오", "흐뭇함(귀여움/예쁨)", "당황/난처",
    "경악", "부담/안_내킴", "서러움", "재미없음", "불쌍함/연민", "놀람", "행복", "불안/걱정",
    "기쁨", "안심/신뢰"]

# ✅ 감성 분석 실행
def run_sentiment_analysis():
    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        forum_data = json.load(f)

    results = []
    for entry in tqdm(forum_data, desc="🔄 감성 분석 진행 중", unit="문장"):
        company = entry["기업명"]
        text = entry["텍스트"]

        encoding = tokenizer(text, truncation=True, max_length=512, return_tensors="pt").to("cuda")
        preds = pipe.tokenizer.decode(encoding["input_ids"][0])
        preds = pipe(preds)[0]

        top_2_preds = sorted(preds, key=lambda x: x["score"], reverse=True)[:2]
        mapped_preds = [{"감정": LABELS[int(p["label"].split("_")[-1])], "점수": p["score"]} for p in top_2_preds]

        results.append({
            "기업명": company,
            "텍스트": text,
            "날짜": entry["날짜"],
            "조회": entry["조회"],
            "추천": entry["추천"],
            "감성분석결과": mapped_preds
        })
    return results

# ✅ FE1: 감성 분석 데이터 정제
def fe1_processing(results):
    seen_texts = set()
    unique_results = []
    for entry in results:
        if entry["텍스트"] not in seen_texts:
            seen_texts.add(entry["텍스트"])
            unique_results.append(entry)
    results = unique_results

    # ✅ 감정 필터링 순서대로 적용
    results = [entry for entry in results if not any(e["감정"] == "없음" for e in entry["감성분석결과"])]
    for entry in results:
        entry["감성분석결과"] = [e for e in entry["감성분석결과"] if e["감정"] not in ["신기함/관심", "아껴주는"]]
    results = [
        entry for entry in results
        if not ("비장함" in {e["감정"] for e in entry["감성분석결과"]} and 
                {"없음", "기대감"} & {e["감정"] for e in entry["감성분석결과"]})
    ]
    for entry in results:
        entry["감성분석결과"] = [e for e in entry["감성분석결과"] if e["점수"] > 0.7]
    results = [entry for entry in results if not any(e["감정"] == "우쭐댐/무시함" for e in entry["감성분석결과"])]
    results = [
        entry for entry in results 
        if not ({"깨달음", "놀람"} <= {e["감정"] for e in entry["감성분석결과"]})
    ]
    results = [entry for entry in results if not any(e["감정"] == "불쌍함/연민" for e in entry["감성분석결과"])]
    results = [entry for entry in results if not any(e["감정"] == "환영/호의" for e in entry["감성분석결과"])]
    results = [
        entry for entry in results 
        if not ({"증오/혐오", "한심함"} <= {e["감정"] for e in entry["감성분석결과"]})
    ]
    for entry in results:
        if len(entry["감성분석결과"]) == 1 and entry["감성분석결과"][0]["감정"] == "깨달음":
            entry["감성분석결과"] = []
    for entry in results:
        entry["감성분석결과"] = [e for e in entry["감성분석결과"] if e["감정"] not in ["존경", "놀람"]]
    results = [entry for entry in results if entry["감성분석결과"]]
    results = [
        entry for entry in results
        if not ("비장함" in {e["감정"] for e in entry["감성분석결과"]} and 
                {"깨달음"} & {e["감정"] for e in entry["감성분석결과"]})
    ]
    for entry in results:
        entry["감성분석결과"] = [e for e in entry["감성분석결과"] if e["감정"] != "비장함"]
    
    results = [entry for entry in results if not any(e["감정"] == "고마움" for e in entry["감성분석결과"])]
    results = [entry for entry in results if entry["감성분석결과"]]
    
    return results

# ✅ FE2: 최종 매매 감정 점수 계산 및 데이터 보정
def fe2_processing(results):
    with open(KIND_FILE, "r", encoding="utf-8") as f:
        kind_data = json.load(f)

    # ✅ 기업별 상장일 정보 저장
    kind_dict = {entry["기업명"]: datetime.strptime(entry["상장일"], "%Y-%m-%d") for entry in kind_data}

    # ✅ 감정 분류
    매도감정 = {"불평/불만", "안타까움/실망", "어이없음", "의심/불신", "불안/걱정", 
                "짜증", "한심함", "화남/분노", "당황/난처", "슬픔", "증오/혐오", "힘듦/지침"}

    매수감정 = {"기대감", "즐거움/신남", "안심/신뢰", "감동/감탄", "기쁨", "깨달음"}

    # ✅ 최종 결과 리스트
    final_results = []
    매매점수_목록 = []

    for entry in results:
        기업명 = entry["기업명"]
        글작성일 = datetime.strptime(entry["날짜"], "%Y/%m/%d")  
        추천수 = int(entry["추천"])

        # ✅ 상장일까지 남은 일자 계산
        if 기업명 in kind_dict:
            상장일 = kind_dict[기업명]
            남은일수 = (상장일 - 글작성일).days
            상장일가중치 = 1 / (1 + max(0, 남은일수))  
        else:
            continue  

        # ✅ 추천 수 가중치 적용
        추천가중치 = math.log(추천수 + 1) + 1  

        # ✅ 초기 매매감정점수 계산
        감성결과 = entry["감성분석결과"]
        매도점수 = []
        매수점수 = []

        for 감정 in 감성결과:
            감정명 = 감정["감정"]
            점수 = 감정["점수"]

            if 감정명 in 매도감정:
                매도점수.append(-점수)  
            elif 감정명 in 매수감정:
                매수점수.append(점수)  

        if 매도점수 and 매수점수:
            초기매매점수 = sum(매수점수) + sum(매도점수)
        elif 매도점수:
            초기매매점수 = sum(매도점수) / len(매도점수)
        elif 매수점수:
            초기매매점수 = sum(매수점수) / len(매수점수)
        else:
            초기매매점수 = 0  

        # ✅ 최종 매매감정점수 계산
        최종매매감정점수 = 초기매매점수 * 상장일가중치 * 추천가중치

        # ✅ 최종 결과 저장
        final_results.append({
            "기업명": 기업명,
            "상장일": 상장일.strftime("%Y-%m-%d"),
            "날짜": entry["날짜"],
            "최종매매감정점수": round(최종매매감정점수, 6)  
        })

        # 🔥 평균 계산을 위해 저장
        매매점수_목록.append(최종매매감정점수)

    # ✅ 평균 매매감정점수 계산 (결측값 대체용)
    if 매매점수_목록:
        평균매매점수 = sum(매매점수_목록) / len(매매점수_목록)
    else:
        평균매매점수 = 0  

    print(f"✅ 평균 매매감정점수 계산 완료: {round(평균매매점수, 6)}")

    # ✅ KIND_38에 있는 기업들은 반드시 포함되도록 결측값 채우기
    existing_companies = {entry["기업명"] for entry in final_results}
    missing_companies = set(kind_dict.keys()) - existing_companies

    for 기업명 in missing_companies:
        상장일 = kind_dict[기업명]

        # ✅ 30일 전부터 7일 간격으로 4개 엔트리 추가
        start_date = 상장일 - timedelta(days=30)
        for i in range(4):  
            날짜 = start_date + timedelta(days=i * 7)
            final_results.append({
                "기업명": 기업명,
                "상장일": 상장일.strftime("%Y-%m-%d"),
                "날짜": 날짜.strftime("%Y/%m/%d"),
                "최종매매감정점수": round(평균매매점수, 6)  
            })

    print(f"✅ KIND_38에 있는 기업 모두 포함 완료! 추가된 기업 개수: {len(missing_companies)}")

    return final_results


def process_daily_average(final_results):
    company_date_scores = defaultdict(lambda: defaultdict(list))

    for entry in final_results:
        기업명 = entry["기업명"]
        상장일 = datetime.strptime(entry["상장일"], "%Y-%m-%d")
        날짜 = datetime.strptime(entry["날짜"], "%Y/%m/%d")
        점수 = entry["최종매매감정점수"]
        
        if (상장일 - 날짜).days <= 30:
            company_date_scores[기업명][날짜].append(점수)

    processed_results = []
    for 기업명, date_scores in company_date_scores.items():
        상장일 = next(entry["상장일"] for entry in final_results if entry["기업명"] == 기업명)

        for 날짜, scores in sorted(date_scores.items()):
            평균점수 = sum(scores) / len(scores)
            processed_results.append({
                "기업명": 기업명,
                "상장일": 상장일,
                "날짜": 날짜.strftime("%Y-%m-%d"),
                "최종매매감정점수": round(평균점수, 6)
            })
    
    return processed_results

def run_38_sentiment_ver1():
    results = run_sentiment_analysis()
    results = fe1_processing(results)
    final_results = fe2_processing(results)

    return final_results

def run38_sentiment_ver2():
    final_results = run_38_sentiment_ver1()
    final_results_avg = process_daily_average(final_results)

    return final_results_avg

# ✅ 전체 실행
if __name__ == "__main__":
    results = run_sentiment_analysis()
    results = fe1_processing(results)
    final_results = fe2_processing(results)
    
    # ✅ FE2 처리 후 결과 저장 (ver1)
    with open(FE2_OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(final_results, f, ensure_ascii=False, indent=4)
    
    print(f"✅ FE2 처리 완료! 저장된 파일: {FE2_OUTPUT_FILE}")

    # ✅ FE2 끝난 후 daily_average 처리 실행
    final_results_avg = process_daily_average(final_results)
    
    # ✅ 최종 결과 저장 (ver2)
    with open(FE2_DAILY_OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(final_results_avg, f, ensure_ascii=False, indent=4)
    
    print(f"✅ 전체 처리 완료! 최종 저장된 파일: {FE2_DAILY_OUTPUT_FILE}")