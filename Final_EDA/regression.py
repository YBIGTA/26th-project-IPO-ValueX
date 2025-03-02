import json
import pandas as pd
import statsmodels.api as sm
from linearmodels.panel import PanelOLS, RandomEffects
from sklearn.linear_model import ElasticNet, BayesianRidge
from xgboost import XGBRegressor
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import r2_score, mean_absolute_error
from statsmodels.stats.outliers_influence import variance_inflation_factor
import numpy as np

# 📌 JSON 데이터 로드
file_path = "regression_block_data_filled.json"

with open(file_path, "r", encoding="utf-8") as f:
    block_data = json.load(f)

# ✅ (1) 패널 회귀에서 인덱스로 사용할 "고정 패널 블록"
fixed_panel_block = ["고정패널"]

# ✅ (2) 독립변수 블록 - 사용할 블록 지정 가능
candidate_blocks = ["상장정보", "재무정보", "재무비율", "재무변화율"]

# ✅ (3) 데이터 변환: 블록을 데이터프레임으로 변환
df_blocks = {block: pd.DataFrame(entries) for block, entries in block_data.items()}

# ✅ (4) 패널 인덱스 설정
df_fixed_panel = pd.concat([df_blocks[block] for block in fixed_panel_block], axis=1)
df_fixed_panel.set_index(["기업명", "상장일", "산업군","섹터"], inplace=True)  # 패널 회귀 인덱스

# ✅ (5) 종속변수 설정
target_block = "종속변수"  # 종속변수가 포함된 블록명
target_variable = "종가대비등락율"  # 종속변수 컬럼명

# ✅ (6) 모든 후보 블록을 합쳐서 한 번에 VIF와 상관계수 계산
df_all_features = pd.concat([df_blocks[block] for block in candidate_blocks], axis=1)

# ✅ (7) 다중공선성 및 상관계수 확인 함수
def calculate_vif(X):
    vif_df = pd.DataFrame()
    vif_df["Feature"] = X.columns
    vif_df["VIF"] = [variance_inflation_factor(X.values, i) for i in range(X.shape[1])]
    return vif_df.sort_values(by="VIF", ascending=False)

# ✅ (8) VIF 분석 후 다중공선성 높은 변수 제거
VIF_threshold = 20
X_all = df_all_features.copy()
scaler = StandardScaler()
X_all_scaled = scaler.fit_transform(X_all)

vif_df = calculate_vif(X_all)
while vif_df["VIF"].max() > VIF_threshold:
    remove_feature = vif_df.iloc[0]["Feature"]
    print(f"⚠ 다중공선성 높은 변수 제거: {remove_feature} (VIF={vif_df.iloc[0]['VIF']:.2f})")
    X_all = X_all.drop(columns=[remove_feature])
    X_all_scaled = scaler.fit_transform(X_all)
    vif_df = calculate_vif(X_all)

print("\n📌 [최종 다중공선성 (VIF) 분석 결과]")
print(vif_df)

# ✅ (9) 안정적인 데이터셋 구축 완료 -> 이제 원하는 블록을 선택 가능!
stable_blocks = list(X_all.columns)  # 안정적인 변수 목록

# ✅ (10) 사용자가 선택할 블록을 터미널에서 입력받기
print("\n📌 사용 가능한 블록 목록:")
print(candidate_blocks)

# 사용자 입력 받기 (쉼표로 구분하여 입력)
user_input = input("\n✅ 사용할 블록들을 입력하세요 (쉼표로 구분, 예: 재무정보, 거시지표): ")
selected_blocks = [block.strip() for block in user_input.split(",")]

# ✅ 입력된 블록이 실제 존재하는 블록인지 검증
invalid_blocks = [block for block in selected_blocks if block not in candidate_blocks]
if invalid_blocks:
    print(f"⚠️ 오류: 존재하지 않는 블록이 포함되어 있습니다: {invalid_blocks}")
    exit()

print(f"\n✅ 선택된 블록: {selected_blocks}")

# ✅ 독립변수 블록 합치기
df_selected = pd.concat([df_blocks[block] for block in selected_blocks], axis=1)

# ✅ 종속변수 추가 (리스트 형태의 데이터 변환)
if target_block in df_blocks:
    df_target = pd.DataFrame(df_blocks[target_block])  # 리스트를 DataFrame으로 변환
    if target_variable in df_target.columns:
        df_selected[target_variable] = df_target[target_variable]
        print(f"\n✅ 종속변수 '{target_variable}' 추가 완료!")
    else:
        print(f"\n⚠️ 오류: 종속변수 블록에 '{target_variable}' 컬럼이 존재하지 않습니다. 현재 컬럼: {df_target.columns.tolist()}")
        exit()
else:
    print(f"\n⚠️ 오류: 블록 '{target_block}'이 존재하지 않습니다.")
    exit()

# ✅ (12) 패널 데이터셋 구성
df_selected = df_selected.join(df_fixed_panel, how="inner")  # 패널 블록 추가
df_selected.set_index(["기업명", "상장일", "산업군","섹터"], inplace=True)  # 패널 회귀 인덱스 설정

# ✅ (13) 다시 VIF 확인 (선택된 블록만)
X = df_selected.drop(columns=[target_variable])
y = df_selected[target_variable]
X_scaled = scaler.fit_transform(X)

vif_selected_df = calculate_vif(X)
print("\n📌 [선택된 블록의 최종 VIF 분석]")
print(vif_selected_df)

# ✅ (14) 회귀 모델 정의 및 실행
models = {
    "Fixed Effects Panel Regression": PanelOLS(y, sm.add_constant(X), entity_effects=True),
    "Random Effects Panel Regression": RandomEffects(y, sm.add_constant(X)),
    "ElasticNet Regression": ElasticNet(alpha=0.1, l1_ratio=0.5),
    "XGBoost Regression": XGBRegressor(n_estimators=100, learning_rate=0.1, random_state=42),
    "Bayesian Ridge Regression": BayesianRidge(),
    "Gradient Boosting Regression": GradientBoostingRegressor(n_estimators=100, learning_rate=0.1, random_state=42)
}

# ✅ (15) 모델 학습 및 평가
results = {}

for model_name, model in models.items():
    if model_name in ["Fixed Effects Panel Regression", "Random Effects Panel Regression"]:
        result = model.fit()
        results[model_name] = result.rsquared_adj
        print(f"\n📌 [{model_name} 결과]")
        print(result.summary)
    else:
        model.fit(X_scaled, y)
        y_pred = model.predict(X_scaled)
        r2 = r2_score(y, y_pred)
        results[model_name] = r2
        print(f"\n📌 [{model_name} 결과]")
        print(f"  R² Score: {r2:.4f}")





