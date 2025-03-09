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
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score, roc_auc_score
from statsmodels.stats.outliers_influence import variance_inflation_factor
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
import seaborn as sns
case_num = "2-2"

# 📌 JSON 데이터 로드
file_path = "classification_block_data_filled.json"

with open(file_path, "r", encoding="utf-8") as f:
    block_data = json.load(f)

# ✅ 로그 파일 설정
log_file_path = f"log/classification_results_case_{case_num}.txt"
with open(log_file_path, "w", encoding="utf-8") as log_file:
    log_file.write("\n✅ JSON 데이터 로드 완료!\n")
    log_file.write(f"📌 저장된 블록 목록: {list(block_data.keys())}\n\n")

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

with open(log_file_path, "a", encoding="utf-8") as log_file:
    log_file.write("\n📌 [초기 다중공선성 (VIF) 분석 결과]\n")
    log_file.write(vif_df.to_string(index=False) + "\n\n")

while vif_df["VIF"].max() > VIF_threshold:
    remove_feature = vif_df.iloc[0]["Feature"]
    with open(log_file_path, "a", encoding="utf-8") as log_file:
        log_file.write("⚠ 다중공선성 높은 변수 제거")
        log_file.write(f"{remove_feature} (VIF={vif_df.iloc[0]['VIF']:.2f})")
    # print(f"⚠ 다중공선성 높은 변수 제거: {remove_feature} (VIF={vif_df.iloc[0]['VIF']:.2f})")
    X_all = X_all.drop(columns=[remove_feature])
    X_all_scaled = scaler.fit_transform(X_all)
    vif_df = calculate_vif(X_all)


# print("\n📌 [최종 다중공선성 (VIF) 분석 결과]")
# print(vif_df)

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
with open(log_file_path, "a", encoding="utf-8") as log_file:
    log_file.write(f"\n✅ 선택된 블록: {selected_blocks}")

# ✅ 독립변수 블록 합치기
df_selected = pd.concat([df_blocks[block] for block in selected_blocks], axis=1)

# ✅ 종속변수 추가 (리스트 형태의 데이터 변환)
if target_block in df_blocks:
    df_target = pd.DataFrame(df_blocks[target_block])  # 리스트를 DataFrame으로 변환
    if target_variable in df_target.columns:
        df_selected[target_variable] = df_target[target_variable]
        print(f"\n✅ 종속변수 '{target_variable}' 추가 완료!")
        with open(log_file_path, "a", encoding="utf-8") as log_file:
            log_file.write(f"\n✅ 종속변수 '{target_variable}' 추가 완료!")
    else:
        print(f"\n⚠️ 오류: 종속변수 블록에 '{target_variable}' 컬럼이 존재하지 않습니다. 현재 컬럼: {df_target.columns.tolist()}")
        exit()
else:
    print(f"\n⚠️ 오류: 블록 '{target_block}'이 존재하지 않습니다.")
    exit()

# ✅ (12) 패널 데이터셋 구성
# df_selected = df_selected.join(df_fixed_panel, how="inner")  # 패널 블록 추가
# df_selected.set_index(["기업명", "상장일", "산업군","섹터"], inplace=True)  # 패널 회귀 인덱스 설정
# ✅ df_fixed_panel의 인덱스를 컬럼으로 변환
df_fixed_panel.reset_index(inplace=True)

# ✅ df_selected도 "기업명", "상장일", "산업군", "섹터" 컬럼을 포함하도록 수정
df_selected["기업명"] = df_fixed_panel["기업명"]
df_selected["상장일"] = df_fixed_panel["상장일"]
df_selected["산업군"] = df_fixed_panel["산업군"]
df_selected["섹터"] = df_fixed_panel["섹터"]

# ✅ merge()를 사용하여 패널 데이터 결합
df_selected = df_selected.merge(df_fixed_panel, on=["기업명", "상장일", "산업군", "섹터"], how="inner")

# ✅ 최종적으로 다시 인덱스를 설정
df_selected.set_index(["기업명", "상장일", "산업군", "섹터"], inplace=True)

# ✅ (13) 다시 VIF 확인 (선택된 블록만)
X = df_selected.drop(columns=[target_variable])
# 종속변수를 이진 분류로 변환 (0 이하 = 0, 0 이상 = 1)
y = (df_selected[target_variable] > 0).astype(int)
X_scaled = scaler.fit_transform(X)

vif_selected_df = calculate_vif(X)
# print("\n📌 [선택된 블록의 최종 VIF 분석]")
# print(vif_selected_df)

with open(log_file_path, "a", encoding="utf-8") as log_file:
    log_file.write("\n📌 [최종 다중공선성 (VIF) 분석 결과]\n")
    log_file.write(vif_df.to_string(index=False) + "\n\n")


# ✅ 데이터셋 분할
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# ✅ 분류 모델 설정
models = {
    "RandomForest": RandomForestClassifier(n_estimators=100, random_state=42),
    "XGBoost": XGBClassifier(n_estimators=100, use_label_encoder=False, eval_metric='logloss', random_state=42),
    "LightGBM": LGBMClassifier(n_estimators=100, random_state=42),
    "LogisticRegression": LogisticRegression(max_iter=1000, random_state=42),
    "GradientBoosting": GradientBoostingClassifier(n_estimators=100, random_state=42)
}

feature_importances = {}
metrics_results = {}

# ✅ 모델 학습 및 평가
for name, model in models.items():
    model.fit(X_train, y_train)
    y_pred = model.predict(X_test)
    
    # 모델 성능 저장
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

# ✅ 피처 중요도 시각화
plt.figure(figsize=(12, 6))
sns.heatmap(importance_df.T, annot=True, cmap="coolwarm", linewidths=0.5)
plt.xlabel("Features")
plt.ylabel("Models")
plt.title("Feature Importance Heatmap Across Models")
plt.show()

# ✅ 결과 저장
importance_df.to_csv(f"feature_importance/classification_feature_importance_results_case_{case_num}.csv", index=True)
metrics_df.to_csv(f"metrics/classification_metrics_results_case_{case_num}.csv", index=True)

print("\n✅ 이진 분류 모델 학습 및 평가 완료!")
