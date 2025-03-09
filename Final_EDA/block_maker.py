import pandas as pd
import json
import os

base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "Regression_modeling", "Regression_data", "regression_df"))

# 📌 3개의 입력 파일 경로
file_paths = {
    "vol": os.path.join(base_dir, "df_regression_vol_sector.csv"),
    "change": os.path.join(base_dir, "df_regression_change_sector.csv"),
    "raw": os.path.join(base_dir, "df_regression_raw_sector_latent.csv"),
}

# 📌 블록 분류 사전 정의
block_mapping = {
    "고정패널": ["기업명", "상장일", "산업군", "섹터"],
    "상장정보": ["단순 기관경쟁률", "의무보유확약비율", "(확정)공모가격", "청약경쟁률", "공모후 발행주식수", "최대주주 소유주 비율", "최대주주 보호예수 기간", "보호예수 비율"],
    "재무정보": ["총자산", "총부채", "자본금", "영업이익", "매출액"],  # ✅ 총자산, 총부채 추가
    "재무비율": ["부채비율", "유동비율", "영업이익률", "당기순이익률", "ROE", "EPS", "EV/영업이익"],
    "재무변화율": ["부채비율 변화율", "유동비율 변화율", "영업이익률 변화율", "당기순이익률 변화율", "매출액 변화율", "ROE 변화율", "EPS 변화율", "EV/영업이익 변화율", "DLR", "PPI (%)"],
    "거시 지표": ["기업경기실사지수(BSI)", "생산자물가 상승률 (PPI)", "소비심리지수(CSI)", "소비자물가 상승률 (CPI)", "수출입 물량지수", "외환보유액", "원달러 환율", "통화량 (M2)", "기준금리", "국고채 금리 스프레드", "무역수지"],
    "산업군시장지수": ["KOSPI_1001", "산업코스피"],
    "커뮤니티종목평가": ["대표매매감정점수"],
    "산업군별뉴스시황": ["호황", "불황"],
    "종속변수": ["종가대비등락율"],
    "latent": ["latent_0","latent_1","latent_2","latent_3","latent_4","latent_5","latent_6","latent_7","latent_8","latent_9","latent_10","latent_11","latent_12","latent_13","latent_14","latent_15","latent_16","latent_17","latent_18","latent_19"]
}

# ✅ 최종 저장할 JSON 구조 (데이터 포함)
final_blocks = {block: [] for block in block_mapping.keys() if block != "거시 지표"}  # 공통 블록 (데이터 저장용)
final_blocks["거시지표_vol"] = []
final_blocks["거시지표_change"] = []
final_blocks["거시지표_raw"] = []


# ✅ 데이터 저장용 리스트 생성
data_storage = {block: [] for block in final_blocks.keys()}  # 데이터 저장

# ✅ 모든 파일에서 공통 블록 데이터 추출
for key, file_path in file_paths.items():
    df = pd.read_csv(file_path)

    # ✅ 총자산 및 총부채 계산
    if "유동자산" in df.columns and "비유동자산" in df.columns:
        df["총자산"] = df["유동자산"] + df["비유동자산"]
    if "유동부채" in df.columns and "비유동부채" in df.columns:
        df["총부채"] = df["유동부채"] + df["비유동부채"]

    # ✅ 기존 변수 삭제 (총자산, 총부채 계산에 사용된 것)
    columns_to_remove = ["유동자산", "비유동자산", "유동부채", "비유동부채"]
    df.drop(columns=[col for col in columns_to_remove if col in df.columns], inplace=True)

    # ✅ 거시 지표는 파일별로 다르게 저장
    if "거시 지표" in block_mapping:
        available_cols = [col for col in block_mapping["거시 지표"] if col in df.columns]
        data_storage[f"거시지표_{key}"] = df[available_cols].to_dict(orient="records")

    # ✅ 공통 블록 저장 (모든 파일에서 동일한 값)
    for block, cols in block_mapping.items():
        if block != "거시 지표":  # 거시 지표는 따로 처리했으므로 제외
            available_cols = [col for col in cols if col in df.columns]

            # ✅ 첫 번째 파일에서만 저장 (모든 파일이 동일하므로 한 번만 저장)
            if not data_storage[block]:
                data_storage[block] = df[available_cols].to_dict(orient="records")

# ✅ JSON 파일 하나로 저장 (데이터 포함)
json_filename = "regression_block_data_with_latent.json"
with open(json_filename, "w", encoding="utf-8") as f:
    json.dump(data_storage, f, indent=4, ensure_ascii=False)

print(f"✅ {json_filename} 저장 완료! (데이터 포함)")

