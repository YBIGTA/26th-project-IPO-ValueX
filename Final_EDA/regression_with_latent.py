import json
import pandas as pd
import statsmodels.api as sm
from sklearn.linear_model import ElasticNet, BayesianRidge
from xgboost import XGBRegressor
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import r2_score
from statsmodels.stats.outliers_influence import variance_inflation_factor
import numpy as np

# 📌 JSON 데이터 로드
file_path = "regression_block_data_with_latent_filled.json"
with open(file_path, "r", encoding="utf-8") as f:
    block_data = json.load(f)

print("\n✅ JSON 데이터 로드 완료!")

# ✅ (1) 블록 데이터프레임 변환
df_blocks = {block: pd.DataFrame(entries) for block, entries in block_data.items()}

# ✅ (2) 고정패널 블록 설정 (인덱스 전용)
fixed_panel_block = "고정패널"
df_fixed_panel = df_blocks.get(fixed_panel_block)
df_fixed_panel.set_index(["기업명", "상장일", "산업군", "섹터"], inplace=True)

# ✅ (3) 독립변수 블록 정의
candidate_blocks = ["상장정보", "재무정보", "재무비율", "재무변화율", "거시지표_vol", "거시지표_change", "거시지표_raw", "산업군시장지수", "커뮤니티종목평가", "산업군별뉴스시황"]
latent_block = "latent"

# ✅ (4) 사용자 입력을 통한 블록 선택
selected_blocks = ["상장정보", "재무정보", "재무비율", "재무변화율", "거시지표_vol", "거시지표_change", "거시지표_raw","커뮤니티종목평가"]

# ✅ (5) 독립변수 데이터프레임 구성
df_X_base = pd.concat([df_blocks[block] for block in selected_blocks if block in df_blocks], axis=1)
df_latent = df_blocks.get(latent_block)  # latent_features 별도 저장
df_X = pd.concat([df_X_base, df_latent], axis=1)  # latent_features 포함

# ✅ (6) 종속변수 추가
target_block = "종속변수"
target_variable = "종가대비등락율"
df_y = df_blocks.get(target_block)
df_X[target_variable] = df_y[target_variable]

# ✅ (7) 데이터 인덱스 설정
df_fixed_panel_reset = df_fixed_panel.reset_index()
df_X = df_X.merge(df_fixed_panel_reset[["기업명", "상장일", "산업군", "섹터"]], how="left", left_index=True, right_index=True)
df_X.set_index(["기업명", "상장일", "산업군", "섹터"], inplace=True)


X_base = df_X.drop(columns=[target_variable] + df_latent.columns.tolist())  # latent_features 제외
scaler = StandardScaler()
X_scaled = scaler.fit_transform(X_base)
X_scaled_df = pd.DataFrame(X_scaled, index=X_base.index, columns=X_base.columns)

# ✅ (9) df_latent 인덱스를 X_base와 동일하게 설정 (MultiIndex 문제 해결)
if len(df_latent) == len(X_base):
    df_latent.index = X_base.index
    print("\n✅ df_latent 인덱스가 X_base와 동일하게 변경되었습니다! 🚀")
else:
    print(f"\n⚠️ df_latent와 X_base의 행 개수가 다릅니다! ({len(df_latent)} vs {len(X_base)})")
    print("⚠️ df_latent의 인덱스를 맞추려면 데이터 정리가 필요합니다.")


X_final = pd.concat([X_scaled_df, df_latent], axis=1)

# ✅ 데이터셋 분할 (MultiIndex 문제 해결)
X_train, X_test, y_train, y_test = train_test_split(X_final, df_X[target_variable], test_size=0.2, random_state=42)

# ✅ X_train, X_test 인덱스 리셋 (MultiIndex 해제)
X_train = X_train.reset_index(drop=True)
X_test = X_test.reset_index(drop=True)


# ✅ 회귀 모델 실행
models = {
    "Linear Regression": sm.OLS(df_X[target_variable], sm.add_constant(X_final)),
    "ElasticNet Regression": ElasticNet(alpha=0.1, l1_ratio=0.5),
    "XGBoost Regression": XGBRegressor(n_estimators=100, learning_rate=0.1, random_state=42),
    "Bayesian Ridge Regression": BayesianRidge(),
    "Gradient Boosting Regression": GradientBoostingRegressor(n_estimators=100, learning_rate=0.1, random_state=42)
}

# ✅ 모델 학습 및 평가
for model_name, model in models.items():
    if model_name == "Linear Regression":
        result = model.fit()
        y_pred = result.predict(sm.add_constant(X_final))
        y_true = df_X[target_variable]
    elif model_name in ["XGBoost Regression", "Gradient Boosting Regression"]:
        # ✅ XGBoost 및 Gradient Boosting만 NumPy 변환
        X_train_np = X_train.to_numpy(dtype=np.float64)
        X_test_np = X_test.to_numpy(dtype=np.float64)
        y_train_np = y_train.to_numpy(dtype=np.float64)
        y_test_np = y_test.to_numpy(dtype=np.float64)

        model.fit(X_train_np, y_train_np)
        y_pred = model.predict(X_test_np)
        y_true = y_test_np
    else:
        model.fit(X_final, df_X[target_variable])
        y_pred = model.predict(X_final)
        y_true = df_X[target_variable]

    r2 = r2_score(y_true, y_pred)
    print(f"\n📌 [{model_name} 결과] R² Score: {r2:.4f}")

print("\n✅ 회귀 분석 완료!")
