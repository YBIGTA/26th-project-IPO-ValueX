import pandas as pd
import os


# ✅ 실행 중인 스크립트의 디렉토리 가져오기
script_dir = os.path.dirname(os.path.abspath(__file__))

# ✅ 파일 경로 설정
data_dir = os.path.join(os.path.expanduser("~"), "Desktop", "26th-project-IPO-ValueX", "NeuralFactor_modeling","data")
df_regression_change_path = os.path.join(data_dir, "df_regression_change.csv")  
latent_features_path = os.path.join(data_dir, "latent_features(final).csv") 
output_path = os.path.join(data_dir, "df_final.csv")

# ✅ 데이터 로드
df_regression_change = pd.read_csv(df_regression_change_path)
latent_features = pd.read_csv(latent_features_path)

# ✅ 날짜 컬럼을 datetime 형식으로 변환
df_regression_change["상장일"] = pd.to_datetime(df_regression_change["상장일"], errors="coerce")
latent_features["date"] = pd.to_datetime(latent_features["date"], errors="coerce")

# ✅ "latent_1" ~ "latent_15" 필드만 선택
latent_features_selected = latent_features[["date"] + [f"latent_{i}" for i in range(1, 16)]]

# ✅ "상장일"과 "date"를 기준으로 병합
df_merged = df_regression_change.merge(
    latent_features_selected, left_on="상장일", right_on="date", how="left"
)

# ✅ "date" 컬럼 제거 (필요 없는 경우)
df_merged.drop(columns=["date"], inplace=True)

# ✅ 병합된 데이터 저장
df_merged.to_csv(output_path, index=False, encoding="utf-8-sig")


print(f"✅ 병합된 데이터 저장 완료: {output_path}")
