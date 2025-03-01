import json
import pandas as pd

# 📌 JSON 파일 로드
file_path = "regression_block_data.json"

with open(file_path, "r", encoding="utf-8") as f:
    block_data = json.load(f)

# ✅ 결측치 처리 (각 블록별 평균값으로 대체, 전체 NaN이면 0으로 채움)
processed_blocks = {}

for block, entries in block_data.items():
    df = pd.DataFrame(entries)  # 블록을 DataFrame으로 변환

    # ✅ 결측치를 해당 블록 내 평균값으로 대체 (전체 NaN이면 0으로 채움)
    for col in df.columns:
        if df[col].isnull().sum() > 0:  # 결측치가 존재하는 경우
            mean_value = df[col].mean()  # 평균값 계산
            if pd.isna(mean_value):  # 만약 평균이 NaN이면 (컬럼 전체가 NaN)
                mean_value = 0  # 기본값 0으로 대체
            
            df[col] = df[col].fillna(mean_value)  # ✅ inplace=True 제거하고 직접 할당

    processed_blocks[block] = df.to_dict(orient="records")  # JSON 변환

# ✅ 처리된 데이터를 다시 JSON으로 저장
output_path = "regression_block_data_filled.json"

with open(output_path, "w", encoding="utf-8") as f:
    json.dump(processed_blocks, f, indent=4, ensure_ascii=False)

print(f"✅ 결측치 대체 완료! 저장된 파일: {output_path}")

