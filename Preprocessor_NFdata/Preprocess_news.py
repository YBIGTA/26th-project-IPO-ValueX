import pandas as pd
import re
import numpy as np
from tqdm import tqdm 

# 국가명 한자 변환 딕셔너리
hanja_to_country = {
    "美": "미국", "軍": "군대", "日": "일본","中": "중국", "英": "영국", "亞": "아시아",
    "獨": "독일", "佛": "프랑스", "加": "캐나다", "露": "러시아", "印": "인도",
    "西": "스페인", "濠": "호주", "韓": "한국", "北": "북한", "南": "남한",
    "港": "홍콩", "台": "대만", "新": "싱가포르", "越": "베트남", "泰": "태국"
}

#------------------------------------------------------------------------------------------------
# 내용이 없거나 나열식 본문 NaN으로 변환

def delete_body(df):
    """
    특정 패턴을 포함하는 제목의 본문(Body)을 NaN으로 변경하는 함수.

    1. 제목이 `[공시]`, `[유가공시]`, `[정정공시]` 등 `[ ]` 안에 "공시"가 포함되면 삭제
    2. 제목이 `[인사]`로 시작하면 삭제
    3. 제목이 `[표]`로 시작하면 삭제
    4. 제목이 `[오늘의 메모]`로 시작하면 삭제
    5. 제목이 `[주간추천주]`로 시작하면 삭제
    6. 제목이 "종목뉴스"로 끝나면 삭제

    :param df: 처리할 DataFrame
    :return: 본문을 수정한 DataFrame
    """

    # 정규표현식 패턴
    pattern_public_notice = r"^\[.*공시.*\]"  
    pattern_personnel_notice = r"^\[인사\]"  
    pattern_table_notice = r"^\[표\]"  
    pattern_today_memo = r"^\[오늘의 메모\]"  
    pattern_weekly_recommendation = r"^\[주간추천주\]"  
    pattern_stock_news = r"종목뉴스$"  

    # 특정 패턴을 포함하는 제목의 Body를 NaN으로 변경
    df = df.copy()
    df.loc[df["Title"].str.match(pattern_public_notice, na=False), "Body_processed"] = np.nan
    df.loc[df["Title"].str.match(pattern_personnel_notice, na=False), "Body_processed"] = np.nan
    df.loc[df["Title"].str.match(pattern_table_notice, na=False), "Body_processed"] = np.nan
    df.loc[df["Title"].str.match(pattern_today_memo, na=False), "Body_processed"] = np.nan
    df.loc[df["Title"].str.match(pattern_weekly_recommendation, na=False), "Body_processed"] = np.nan
    df.loc[df["Title"].str.endswith("종목뉴스", na=False), "Body_processed"] = np.nan

    return df

#------------------------------------------------------------------------------------------------
#  광고글 삭제

def remove_advertisements(df):
    """
    1. 제목(Title)과 본문(Body)에 특정 키워드(급등주, 테마, 종목)가 모두 포함되면 해당 행을 삭제.
    2. 본문(Body)에 "◆ 새해 마켓레이더가 다양해집니다…" 이후의 모든 내용을 삭제.
    3. NaN 값은 유지하며 처리.

    :param df: 처리할 DataFrame
    :return: 광고 게시글을 NaN 또는 본문 일부 제거한 DataFrame
    """
    # 광고성 키워드 리스트
    ad_keywords = ["급등주", "테마", "종목"]
    
    # 광고성 키워드가 모두 포함된 행 찾기
    mask = df["Title"].astype(str).str.contains(ad_keywords[0], na=False) & \
           df["Title"].astype(str).str.contains(ad_keywords[1], na=False) & \
           df["Title"].astype(str).str.contains(ad_keywords[2], na=False)

    mask |= df["Body"].astype(str).str.contains(ad_keywords[0], na=False) & \
            df["Body"].astype(str).str.contains(ad_keywords[1], na=False) & \
            df["Body"].astype(str).str.contains(ad_keywords[2], na=False)

    # 광고성 키워드 포함된 행을 NaN으로 변경 (하지만 기존 NaN은 유지)
    df = df.copy()
    df.loc[mask, ["Title", "Body_processed"]] = np.nan

    # 본문에서 "◆ 새해 마켓레이더가 다양해집니다…" 이후 내용 삭제 (NaN 값은 유지)
    df.loc[:, "Body_processed"] = df["Body"].astype(str).apply(lambda x: re.sub(r"◆ 새해 마켓레이더가 다양해집니다….*", "", x) if x != "nan" else x)

    return df

#------------------------------------------------------------------------------------------------
#  한자 변환

def convert_hanja(text):
    """뉴스 기사에서 한자로 표현된 국가명을 한글로 변환"""
    if not isinstance(text, str):  
        return text  # 원래 값 반환 (NaN 또는 숫자인 경우)

    # 기본 변환 (美 → 미국, 軍 → 군대 등)
    for hanja, korean in hanja_to_country.items():
        text = text.replace(hanja, korean)

    # # "中" 변환 (문맥 고려: "중국"이 포함된 경우만 변환)
    # if "중국" in text:
    #     text = re.sub(r"中(?=.*중국)(?!\.$)", "중국", text)

    return text

#------------------------------------------------------------------------------------------------
#  쓸데 없는 내용 삭제

def preprocess_body(text):
    if not isinstance(text, str):  
        return text  # 원래 값 반환 (NaN 또는 숫자인 경우)
    
    text_cleaned = text

    # 1️ ⓒ 포함된 행과 그 이후 모든 행 삭제
    text_cleaned = re.sub(r"ⓒ.*", "ⓒ", text_cleaned)  # ⓒ 이후 모든 내용 → ⓒ만 남기기
    text_cleaned = text_cleaned.split("\n")  # 줄 단위로 분할
    text_cleaned = "\n".join(line for line in text_cleaned if "ⓒ" not in line)  # ⓒ가 포함된 행 삭제

    # 2️ [] 형태의 글 모두 삭제 (언론사, 추천, 이미지 출처 등)
    text_cleaned = re.sub(r"\[.*?\]", "", text_cleaned)

    # 3️ @가 포함된 행 삭제
    text_cleaned = "\n".join([line for line in text_cleaned.split("\n") if "@" not in line])

    # 4️ "기자"로 끝나는 행 삭제
    text_cleaned = "\n".join([line for line in text_cleaned.split("\n") if not re.search(r"(\s기자\s|기자$)", line)])

    # 5️ **"/" 이후 기자이름 + '기자'로 끝나는 행 삭제** 
    text_cleaned = "\n".join([line for line in text_cleaned.split("\n") if not re.search(r"/\s*[가-힣]+\s*기자$", line)])

    # 6️ 특정 패턴을 포함하는 문장 자체 삭제
    remove_patterns = [
        r"▶.*",  # ▶로 시작하는 모든 문장 삭제
        r"☞.*",  # ☞로 시작하는 문장 삭제
        r"【.*?】",  # 【 앵커멘트 】 등 삭제
        r".*? 기자입니다\.",  # "000 기자입니다." 삭제
        r".*? 기자가 취재했습니다\.",  # "000 기자가 취재했습니다." 삭제
        r".*? 추천$",  # "추천"으로 끝나는 문장 삭제
        r".*? 이 기사는 .*",  # "이 기사는 ~" 문장 전체 삭제
        r".*? 사진제공=.*",  # "사진제공" 포함된 줄 삭제
        r".*? 그래픽.*",  # "[그래픽]" 포함된 줄 삭제
        r"사진\s*=",  # "사진 =" 삭제
        r"\(이하 .*?\)",  # "(이하 ~)" 패턴 삭제
        r"한국경제|한경로보뉴스",  # "한국경제"와 "한경로보뉴스" 삭제
        r"순매수 상위 종목\s*\n\n",  # "순매수 상위 종목"이 단독으로 있고 \n\n 있는 경우 삭제
        r"순매도 상위 종목\s*\n\n",  # "순매도 상위 종목"이 단독으로 있고 \n\n 있는 경우 삭제
        r"스톡봇 기자.*",  # "스톡봇 기자" 포함 이후 모든 줄 삭제
        r"!\[image.*\]\(attachment:.*?\)",  # "[image.png](attachment:..)" 같은 이미지 첨부 태그 삭제
    ]
    for pattern in remove_patterns:
        text_cleaned = re.sub(pattern, "", text_cleaned)

    # 7 **인터뷰 관련 문장 삭제 (▶ 인터뷰 : ... 다음 줄도 삭제)**
    lines = text_cleaned.split("\n")
    filtered_lines = []
    skip_next = False  # 인터뷰 다음 줄도 삭제하기 위한 플래그

    for line in lines:
        if "▶ 인터뷰 :" in line:
            skip_next = True  # 다음 줄도 삭제해야 함
            continue
        if skip_next:
            skip_next = False
            continue  # 인터뷰 다음 줄도 삭제
        filtered_lines.append(line)

    text_cleaned = "\n".join(filtered_lines)

    # 8️ **8단어 미만인데 문장 끝에 '.' 없으면 행 삭제**
    lines = text_cleaned.split("\n")
    text_cleaned = "\n".join(
        line for line in lines if len(line.split()) >= 8 or line.strip().endswith(".")
    )

    # 9️ **전체 본문 정제 후 단어 개수가 20개 미만이면 NaN 처리**
    word_count = len(text_cleaned.split())
    if word_count < 20:
        return np.nan  # NaN 값 처리

    # 불필요한 공백 제거
    text_cleaned = re.sub(r"\s{2,}", " ", text_cleaned).strip()

    return text_cleaned

#------------------------------------------------------------------------------------------------
#  특수문자 제거

def remove_special_characters(text):
    """특수문자를 제거하는 함수"""
    if not isinstance(text, str):  
        return text  # NaN 또는 숫자인 경우 원래 값 반환
    
    return re.sub(r"[^\w\s]", "", text)  # 특수문자 제거


def run_preprocess_naver(input_df):
    """네이버 뉴스 데이터 전처리 후 저장"""
    try:
        df = input_df
        df = df.copy()

        df["Body_processed"] = df["Body"]

        # 제목 및 광고성 문구 처리
        print("🚀 제목 및 광고성 문구 필터링 중...")
        df = delete_body(df)
        df = remove_advertisements(df)

        # ✅ tqdm을 사용하여 단일 progress bar로 적용
        print("🚀 본문 전처리 중...")
        with tqdm(total=len(df), desc="📄 텍스트 전처리 진행") as pbar:
            for index, row in df.iterrows():
                processed_text = row["Body_processed"]
                processed_text = convert_hanja(processed_text)
                processed_text = preprocess_body(processed_text)
                processed_text = remove_special_characters(processed_text)

                df.at[index, "Body_processed"] = processed_text
                pbar.update(1)  # ✅ 한 단계 완료 후 진행률 업데이트

        return df

    except Exception as e:
        print(f"❌ 오류 발생: {e}")