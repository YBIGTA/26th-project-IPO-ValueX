import json
import pandas as pd
import statsmodels.api as sm
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from xgboost import XGBClassifier
from lightgbm import LGBMClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split
from sklearn.inspection import permutation_importance
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score, classification_report, roc_auc_score
from statsmodels.stats.outliers_influence import variance_inflation_factor
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
case_num = 7

# 📌 JSON 데이터 로드
file_path = "classification_block_data_filled.json"

with open(file_path, "r", encoding="utf-8") as f:
    block_data = json.load(f)

# ✅ 로그 파일 설정
log_file_path = f"log/classification_results_case_{case_num}.txt"
with open(log_file_path, "w", encoding="utf-8") as log_file:
    log_file.write("\n✅ JSON 데이터 로드 완료!\n")
    log_file.write(f"📌 저장된 블록 목록: {list(block_data.keys())}\n\n")

# print("\n✅ JSON 데이터 로드 완료!")
# print(f"📌 저장된 블록 목록: {list(block_data.keys())}")

# ✅ (1) 패널 블록 설정 (인덱스로만 활용)
fixed_panel_block = "고정패널"

# ✅ (2) 독립변수 블록 - 사용할 블록 지정 가능
candidate_blocks = ["상장정보", "재무정보", "재무비율", "재무변화율", "거시지표_vol", "거시지표_change", "거시지표_raw", "산업군시장지수", "커뮤니티종목평가", "산업군별뉴스시황"]

# ✅ (3) 데이터 변환: JSON 데이터를 DataFrame으로 변환
df_blocks = {block: pd.DataFrame(entries) for block, entries in block_data.items()}

# ✅ (4) 패널 블록 인덱스 설정
df_fixed_panel = df_blocks[fixed_panel_block].copy()
df_fixed_panel.set_index(["기업명", "상장일", "산업군", "섹터"], inplace=True)

# ✅ (5) 종속변수 설정
target_block = "종속변수"
target_variable = "종가대비등락율"

# ✅ (6) 블록 선택 (사용자가 선택한 블록만 사용)
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
with open(log_file_path, "a", encoding="utf-8") as log_file:
    log_file.write(f"\n✅ 선택된 블록: {selected_blocks}")

# ✅ (7) 독립변수 블록 합치기
df_selected = pd.concat([df_blocks[block] for block in selected_blocks], axis=1)

# ✅ (8) 종속변수 추가 및 이진 변환 (20% 이상이면 1, 이하면 0)
df_target = df_blocks[target_block]
df_selected[target_variable] = (df_target[target_variable] > 20).astype(int)
print("\n✅ 종속변수를 이진 변수로 변환 완료!")

# ✅ (9) 패널 데이터 인덱스 설정
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

# print("\n📌 [최종 VIF 분석 결과]")
# print(vif_df)

with open(log_file_path, "a", encoding="utf-8") as log_file:
    log_file.write("\n📌 [최종 다중공선성 (VIF) 분석 결과]\n")
    log_file.write(vif_df.to_string(index=False) + "\n\n")

# ✅ (12) 데이터셋 분할
X_train, X_test, y_train, y_test = train_test_split(X, df_selected[target_variable], test_size=0.2, random_state=42)

# ✅ (13) 분류 모델 설정
models = {
    "RandomForest": RandomForestClassifier(n_estimators=100, random_state=42),
    "XGBoost": XGBClassifier(n_estimators=100, use_label_encoder=False, eval_metric='logloss', random_state=42),
    "LightGBM": LGBMClassifier(n_estimators=100, random_state=42),
    "LogisticRegression": LogisticRegression(max_iter=1000, random_state=42),
    "GradientBoosting": GradientBoostingClassifier(n_estimators=100, random_state=42)
}

feature_importances = {}
metrics_results = {}

# ✅ (14) 모델 학습 및 평가
for name, model in models.items():
    model.fit(X_train, y_train)
    y_pred = model.predict(X_test)
    
    # 평가 지표 저장
    metrics_results[name] = {
        "Accuracy": accuracy_score(y_test, y_pred),
        "Precision": precision_score(y_test, y_pred, average='macro'),
        "Recall": recall_score(y_test, y_pred, average='macro'),
        "F1-score": f1_score(y_test, y_pred, average='macro'),
        "ROC AUC": roc_auc_score(y_test, model.predict_proba(X_test)[:, 1]) if hasattr(model, "predict_proba") else None
    }
    
    # 피처 중요도 저장
    if hasattr(model, 'feature_importances_'):
        importances = model.feature_importances_
    else:
        importances = permutation_importance(model, X_test, y_test, n_repeats=10, random_state=42).importances_mean
    
    feature_importances[name] = importances

# 모델별 피처 중요도 정규화
for model in feature_importances:
    feature_importances[model] = feature_importances[model] / np.sum(feature_importances[model])

# ✅ DataFrame 변환
importance_df = pd.DataFrame(feature_importances, index=X.columns)
importance_df = importance_df.sort_values(by="RandomForest", ascending=False)
metrics_df = pd.DataFrame(metrics_results).T

# 한글 폰트 설정
plt.rc('font', family='AppleGothic')
plt.rcParams['axes.unicode_minus'] = False  # 마이너스(-) 기호 깨짐 방지

# ✅ (15) 피처 중요도 히트맵
importance_df = pd.DataFrame(feature_importances, index=X.columns).sort_values(by="RandomForest", ascending=False)
plt.figure(figsize=(12, 6))
sns.heatmap(importance_df.T, annot=True, cmap="coolwarm", linewidths=0.5)
plt.xlabel("Features")
plt.ylabel("Models")
plt.title("Feature Importance Heatmap Across Models")
plt.show()

# ✅ 결과 저장
importance_df.to_csv(f"feature_importance/classification_plus_feature_importance_results_case_{case_num}.csv", index=True)
metrics_df.to_csv(f"metrics/classification_plus_metrics_results_case_{case_num}.csv", index=True)

print("\n✅ 분류 모델 학습 및 평가 완료!")