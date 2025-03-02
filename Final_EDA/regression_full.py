#vif를 활용한 피처선정과 p밸류를 통한 피처셀렉션이 순차적으로 들어감.

import json
import pandas as pd
import statsmodels.api as sm
from sklearn.linear_model import ElasticNet, BayesianRidge
from xgboost import XGBRegressor
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import r2_score, mean_absolute_error, mean_squared_error
from statsmodels.stats.outliers_influence import variance_inflation_factor
import numpy as np

# 📌 JSON 데이터 로드
file_path = "regression_block_data_filled.json"

with open(file_path, "r", encoding="utf-8") as f:
    block_data = json.load(f)

print("\n✅ JSON 데이터 로드 완료!")
print(f"📌 저장된 블록 목록: {list(block_data.keys())}")

# ✅ (1) 고정 패널 블록 (인덱스로만 사용)
fixed_panel_block = "고정패널"

# ✅ (2) 독립변수 블록 - 사용자 지정 가능
candidate_blocks = ["상장정보", "재무정보", "재무비율", "재무변화율", "거시지표_vol", "거시지표_change", "거시지표_raw", "산업군시장지수", "커뮤니티종목평가", "산업군별뉴스시황"]

# ✅ (3) JSON 데이터를 데이터프레임으로 변환
df_blocks = {block: pd.DataFrame(entries) for block, entries in block_data.items()}

# ✅ (4) 고정 패널 블록 설정 (인덱스로만 활용)
if fixed_panel_block in df_blocks:
    df_fixed_panel = pd.DataFrame(df_blocks[fixed_panel_block])
    df_fixed_panel.set_index(["기업명", "상장일", "산업군", "섹터"], inplace=True)
    print("\n✅ '고정패널' 인덱스 설정 완료!")
else:
    print("⚠️ '고정패널' 블록이 JSON에 존재하지 않습니다!")
    exit()

# ✅ (5) 종속변수 설정
target_block = "종속변수"
target_variable = "종가대비등락율"

# ✅ (6) 사용자 입력을 통한 블록 선택
print("\n📌 사용 가능한 블록 목록:")
print(candidate_blocks)

user_input = input("\n✅ 사용할 블록들을 입력하세요 (쉼표로 구분, 예: 재무정보, 산업군시장지수): ")
selected_blocks = [block.strip() for block in user_input.split(",")]

# ✅ 블록 유효성 체크
invalid_blocks = [block for block in selected_blocks if block not in candidate_blocks]
if invalid_blocks:
    print(f"⚠️ 오류: 존재하지 않는 블록이 포함되어 있습니다: {invalid_blocks}")
    exit()

print(f"\n✅ 선택된 블록: {selected_blocks}")

# ✅ (7) 독립변수 블록 합치기
df_selected = pd.concat([df_blocks[block] for block in selected_blocks if block in df_blocks], axis=1)
print("\n✅ 독립변수 블록 합치기 완료!")
print(f"📌 현재 df_selected 컬럼 목록: {df_selected.columns.tolist()}")

# ✅ (8) 종속변수 추가
if target_block in df_blocks:
    df_target = pd.DataFrame(df_blocks[target_block])
    if target_variable in df_target.columns:
        df_selected[target_variable] = df_target[target_variable]
        
    else:
        exit()
else:
    exit()

# ✅ (9) 인덱스 설정
df_fixed_panel_reset = df_fixed_panel.reset_index()
df_selected = df_selected.merge(df_fixed_panel_reset[["기업명", "상장일", "산업군", "섹터"]], how="left", left_index=True, right_index=True)
df_selected.set_index(["기업명", "상장일", "산업군", "섹터"], inplace=True)

# ✅ (10) VIF 분석 및 다중공선성 제거
def calculate_vif(X):
    vif_df = pd.DataFrame()
    vif_df["Feature"] = X.columns
    vif_df["VIF"] = [variance_inflation_factor(X.values, i) for i in range(X.shape[1])]
    return vif_df.sort_values(by="VIF", ascending=False)

VIF_threshold = 20
X = df_selected.drop(columns=[target_variable])
scaler = StandardScaler()
X_scaled = scaler.fit_transform(X)

vif_df = calculate_vif(X)
removed_vif_features = []
while vif_df["VIF"].max() > VIF_threshold and X.shape[1] > 1:
    remove_feature = vif_df.iloc[0]["Feature"]
    removed_vif_features.append(remove_feature)
    X = X.drop(columns=[remove_feature])
    X_scaled = scaler.fit_transform(X)
    vif_df = calculate_vif(X)

print("\n📌 [최종 다중공선성 (VIF) 분석 결과]")
print(vif_df.to_string(index=False))

# ✅ P-value 기반 변수 제거
p_value_threshold = 0.05
removed_p_features = []

ols_model = sm.OLS(df_selected[target_variable], sm.add_constant(X))
ols_result = ols_model.fit()
p_values = ols_result.pvalues.drop("const")

while p_values.max() > p_value_threshold and X.shape[1] > 1:
    remove_feature = p_values.idxmax()
    removed_p_features.append(remove_feature)
    X = X.drop(columns=[remove_feature])
    ols_model = sm.OLS(df_selected[target_variable], sm.add_constant(X))
    ols_result = ols_model.fit()
    p_values = ols_result.pvalues.drop("const")

print("\n📌 [최종 OLS 결과]")
print(ols_result.summary())

# ✅ **블록별 최종 사용된 변수 출력**
block_used_features = {block: [] for block in selected_blocks}
for feature in X.columns:
    for block in selected_blocks:
        if feature in df_blocks[block].columns:
            block_used_features[block].append(feature)

print("\n📌 [블록별 최종 사용된 변수 목록]")
for block, features in block_used_features.items():
    print(f"🔹 {block}: {features}")

# ✅ 데이터셋 분할 (XGBoost & Gradient Boosting만 적용)
X_train, X_test, y_train, y_test = train_test_split(X, df_selected[target_variable], test_size=0.2, random_state=42)

# ✅ 회귀 모델 실행
models = {
    "Linear Regression": sm.OLS(df_selected[target_variable], sm.add_constant(X)),
    "ElasticNet Regression": ElasticNet(alpha=0.1, l1_ratio=0.5),
    "XGBoost Regression": XGBRegressor(n_estimators=100, learning_rate=0.1, random_state=42),
    "Bayesian Ridge Regression": BayesianRidge(),
    "Gradient Boosting Regression": GradientBoostingRegressor(n_estimators=100, learning_rate=0.1, random_state=42)
}

# ✅ 모델 학습 및 평가
for model_name, model in models.items():
    

    if model_name == "Linear Regression":
        result = model.fit()
        y_pred = result.predict(sm.add_constant(X))
        y_true = df_selected[target_variable]
    elif model_name in ["XGBoost Regression", "Gradient Boosting Regression"]:
        model.fit(X_train, y_train)
        y_pred = model.predict(X_test)
        y_true = y_test
    else:
        model.fit(X, df_selected[target_variable])
        y_pred = model.predict(X)
        y_true = df_selected[target_variable]

    r2 = r2_score(y_true, y_pred)
    print(f"\n📌 [{model_name} 결과] R² Score: {r2:.4f}")

print("\n✅ 회귀 분석 완료!")
















