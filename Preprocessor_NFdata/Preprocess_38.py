import json
import re
from datetime import datetime, timedelta
from collections import defaultdict

# ✅ Step 1: 날짜 필터링 (datecutting.py 기능)
def filter_by_date(input_data, kind_data):
    kind_dict = {entry["기업명"]: datetime.strptime(entry["상장일"], "%Y-%m-%d") for entry in kind_data}
    valid_companies = set(kind_dict.keys())
    
    def parse_date(date_str):
        date_str = date_str.strip()
        
        if not date_str:
            return None  # 빈 문자열이면 None 반환
        
        for fmt in ("%Y/%m/%d", "%Y-%m-%d", "%m/%d/%Y"):
            try:
                return datetime.strptime(date_str, fmt)
            except ValueError:
                continue
        
        print(f"🚨 경고: '{date_str}'는 인식할 수 없는 날짜 형식입니다. 데이터 무시.")
        return None  # 변환 실패 시 None 반환
    
    filtered_data = []
    for entry in input_data:
        company = entry.get("기업명", "").strip()
        post_date_str = entry.get("날짜", "").strip()

        if not post_date_str:
            print(f"🚨 경고: '{company}'의 날짜 필드가 없음. 데이터 무시.")
            continue

        post_date = parse_date(post_date_str)
        if post_date is None:
            continue

        if company in valid_companies:
            ipo_date = kind_dict[company]
            start_date = ipo_date - timedelta(days=30)

            if start_date <= post_date <= ipo_date:
                filtered_data.append(entry)
    
    return filtered_data

# ✅ Step 2: 종목토론방 용어 변환 (transform_word.py 기능)
def transform_text(data):
    stock_terms = {
        # 주가 상승 관련
    "따상": "상한가 2번", "따블": "2배 상승", "강상따": "강한 상한가 후 매수",
    "쩜상": "상한가에서 소폭 움직임", "수직상승": "급등", "연상": "연속 상한가",
    "장대양봉": "주가의 큰 폭 상승", "시초가": "장 시작 가격", "떡상" : "주가가 많이 오름",

    # 주가 하락 관련
    "따하": "하한가 2번", "하따": "하한가에서 매수", "장대 음봉": "긴 하락 캔들",
    "물량 출회": "대량 매도 발생", "손절": "손실 확정 매도", "익절": "이익 실현 매도",
    "물림": "고점 매수 후 하락", "물타기": "추가 매수", "불타기": "상승장에서 추가 매수",
    "한강": "전재산 손실", "초상": "초상집 분위기", "떡락" : "주가가 많이 떨어짐", "물렸다" : "손해 많이 봤다", "물림" : "손해 많이 봄",

    # 투자 심리 관련
    "존버": "장기 보유", "뇌동매매": "감정적 매매", "몰빵": "전 재산 투자",
    "개미": "개인 투자자", "기관": "기관 투자자", "외인": "외국인 투자자",
    "양전": "주가가 전일 대비 상승", "음전": "주가가 전일 대비 하락",
    "개잡주": "변동성이 큰 종목", "잡주": "저평가된 종목", "대장주": "업종 대표주",
    "테마주": "특정 이슈로 움직이는 주식", "작전주": "세력들이 인위적으로 올리는 주식",
    "VI 발동": "변동성 완화 장치 발동", "공매도": "주식을 빌려서 매도 후, 하락 시 차익 실현",
    "안티": "특정 종목에 대해 부정적인 사람들", "던졌다" :" 망해서 포기했다", "홀딩" : "사고팔지 말고 기다림",

    # 감정 표현 (부정적 분위기 표현)
    "…": "실망", "ㅜㅜㅜ": "슬픔", "ㅠㅠㅠ": "좌절", "하…": "한숨",
    "허허…": "어이없음", "후…": "실망", "참나…": "황당", "아니…": "당황",
    "어휴…": "답답함", "이게 뭐야…": "기대가 무너짐", "망했다…": "절망",
    "끝났다…": "희망 없음", "답 없다…": "해결책 없음",
    "힘빠지네…": "지침", "멘붕": "정신적 충격", "개같이" : "엄청나게"
    }
    
    def clean_text(text):
        if text is None:
            return ""
        text = text.replace("\n", " ")  # 줄바꿈 제거
        text = re.sub(r"[\~\-\=\+\*\[\]\(\)]", "", text)  # 특수 문자 제거 (~, -, =, +, *, [], (), 등)
        text = re.sub(r"ㅋ{2,}", "ㅋㅋ", text)  # ㅋㅋㅋ 같은 반복 문자 축소
        text = re.sub(r"ㅎ{2,}", "ㅎㅎ", text)  # ㅎㅎㅎ 같은 반복 문자 축소
        text = re.sub(r"\?{2,}", "?", text)  # ??? 같은 반복 문자 축소
        text = re.sub(r"\!{2,}", "!", text)  # !!! 같은 반복 문자 축소
        text = re.sub(r"[\s]+", " ", text).strip()  # 연속된 공백 제거
        return text 
    
    def preprocess_text(text):
        for term, replacement in stock_terms.items():
            text = text.replace(term, replacement)
        return clean_text(text)
    
    for entry in data:
        entry["내용"] = preprocess_text(entry["내용"])
    return data

# ✅ Step 3: 데이터 그룹화 및 정리 (form_to_train.py 기능, 개선 버전)
def group_and_clean_data(data):
    grouped_data = defaultdict(list)
    company_views = defaultdict(list)
    
    def safe_int(value):
        try:
            return int(value) if value and str(value).isdigit() else None
        except ValueError:
            return None
    
    for entry in data:
        if (views := safe_int(entry.get("조회"))) and views > 0:
            company_views[entry["기업명"]].append(views)
    
    global_avg_views = sum(v for views in company_views.values() for v in views) // len(company_views) if company_views else 0
    company_avg_views = {company: (sum(views) // len(views) if views else global_avg_views) for company, views in company_views.items()}
    
    def is_valid_text(text):
        text = text.strip()
        if re.fullmatch(r"\.*", text):  
            return False  
        if len(text.split()) == 1:  
            return False  
        if len(text.replace(" ", "")) <= 5:  
            return False  
        words = text.split()
        midpoint = len(words) // 2
        if len(words) > 4 and words[:midpoint] == words[midpoint:]:  
            return False
        return True  
    
    for entry in data:
        company = entry["기업명"]
        text = f"{entry.get('제목', '')} {entry.get('내용', '')}".strip()
        
        # ✅ 조회수 처리 (빈 값이거나 0이면 평균값 사용)
        views = safe_int(entry.get("조회"))  # 조회수 변환
        if views is None or views == 0:  # 조회수가 없거나 0이면 평균 조회수 사용
            views = company_avg_views.get(company, global_avg_views)

        # ✅ 추천수 처리 (빈 값이면 0)
        recommendations = safe_int(entry.get("추천")) or 0

        
        if not is_valid_text(text):
            continue  
        
        post_info = {
            "텍스트": text,
            "날짜": entry.get("날짜", ""),
            "조회": views,
            "추천": recommendations
        }
        grouped_data[company].append(post_info)
    
    return [{"기업명": company, "글목록": posts} for company, posts in grouped_data.items() if posts]

# ✅ Step 4: KOTE 감성 분석 입력 데이터 변환 (For_kote_input.py 기능)
def create_kote_input(data):
    return [{"기업명": company_data["기업명"], "텍스트": post["텍스트"], "날짜": post["날짜"], "조회": post["조회"], "추천": post["추천"]} for company_data in data for post in company_data["글목록"]]

def run_preprocess_38(input_data, kind_data):
    
    filtered_data = filter_by_date(input_data, kind_data)
    transformed_data = transform_text(filtered_data)
    grouped_data = group_and_clean_data(transformed_data)
    kote_input_data = create_kote_input(grouped_data)

    print("✅ 전체 전처리 완료! 결과 파일: 38_KOTE_Input.json")
    
    return kote_input_data



# ✅ 전체 전처리 실행
if __name__ == "__main__":

    with open("Non_Finance_data/38/38_ver1.json", "r", encoding="utf-8") as f:
        input_data = json.load(f)
    with open("Non_Finance_data/38/KIND_38.json", "r", encoding="utf-8") as f:

        kind_data = json.load(f)
    
    filtered_data = filter_by_date(input_data, kind_data)
    transformed_data = transform_text(filtered_data)
    grouped_data = group_and_clean_data(transformed_data)
    kote_input_data = create_kote_input(grouped_data)
    
    with open("38new_KOTE_Input.json", "w", encoding="utf-8") as f:
        json.dump(kote_input_data, f, ensure_ascii=False, indent=4)
    

    print("✅ 전체 전처리 완료! 결과 파일: 38_KOTE_Input.json")


