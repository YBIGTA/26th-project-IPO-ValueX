import json
import pandas as pd
import statsmodels.api as sm
from sklearn.preprocessing import StandardScaler
from statsmodels.stats.outliers_influence import variance_inflation_factor
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score, classification_report, roc_auc_score
import numpy as np

# 📌 JSON 데이터 로드
file_path = "regression_block_data_with_latent_filled.json"

with open(file_path, "r", encoding="utf-8") as f:
    block_data = json.load(f)

print("\n✅ JSON 데이터 로드 완료!")

# ✅ (1) 고정 패널 블록 (인덱스로만 사용)
fixed_panel_block = "고정패널"

# ✅ (2) 독립변수 블록 - 사용자 지정 가능
candidate_blocks = ["상장정보", "재무정보", "재무비율", "재무변화율", "거시지표_vol", "거시지표_change", "거시지표_raw", "산업군시장지수", "커뮤니티종목평가", "산업군별뉴스시황","latent"]

# ✅ (3) JSON 데이터를 데이터프레임으로 변환
df_blocks = {block: pd.DataFrame(entries) for block, entries in block_data.items()}

# ✅ (4) 고정 패널 블록 설정 (인덱스로만 활용)
if fixed_panel_block in df_blocks:
    df_fixed_panel = pd.DataFrame(df_blocks[fixed_panel_block])
    df_fixed_panel.set_index(["기업명", "상장일", "산업군", "섹터"], inplace=True)
    print("\n✅ '고정패널' 인덱스 설정 완료!")

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

# ✅ (7) 독립변수 블록 합치기 (사용자 선택 블록만)
df_selected = pd.concat([df_blocks[block] for block in selected_blocks if block in df_blocks], axis=1)
print("\n✅ 독립변수 블록 합치기 완료!")

# ✅ (8) 종속변수 추가
if target_block in df_blocks:
    df_target = pd.DataFrame(df_blocks[target_block])
    if target_variable in df_target.columns:
        df_selected[target_variable] = df_target[target_variable]
    else:
        print("⚠️ 종속변수가 JSON 파일에 없습니다.")
        exit()
else:
    print("⚠️ 종속변수 블록이 JSON에 없습니다.")
    exit()

# ✅ (9) 종속변수를 3개 구간으로 변환
df_selected[target_variable] = pd.cut(
    df_selected[target_variable], 
    bins=[-float("inf"), 20, 150, float("inf")], 
    labels=[0, 1, 2]
).astype(int)

print("\n✅ 종속변수를 3개 구간으로 변환 완료!")

# ✅ (10) 인덱스 설정
df_fixed_panel_reset = df_fixed_panel.reset_index()
df_selected = df_selected.merge(df_fixed_panel_reset[["기업명", "상장일", "산업군", "섹터"]], how="left", left_index=True, right_index=True)
df_selected.set_index(["기업명", "상장일", "산업군", "섹터"], inplace=True)

# # ✅ (11) VIF 분석 및 다중공선성 제거
# def calculate_vif(X):
#     vif_df = pd.DataFrame()
#     vif_df["Feature"] = X.columns
#     vif_df["VIF"] = [variance_inflation_factor(X.values, i) for i in range(X.shape[1])]
#     return vif_df.sort_values(by="VIF", ascending=False)

# VIF_threshold = 20
X = df_selected.drop(columns=[target_variable])
scaler = StandardScaler()
X_scaled = scaler.fit_transform(X)
X_scaled_df = pd.DataFrame(X_scaled, index=X.index, columns=X.columns)
# vif_df = calculate_vif(X)
# removed_vif_features = []
# while vif_df["VIF"].max() > VIF_threshold and X.shape[1] > 1:
#     remove_feature = vif_df.iloc[0]["Feature"]
#     removed_vif_features.append(remove_feature)
#     X = X.drop(columns=[remove_feature])
#     X_scaled = scaler.fit_transform(X)
#     vif_df = calculate_vif(X)

# print("\n📌 [최종 다중공선성 (VIF) 분석 결과]")
# print(vif_df.to_string(index=False))

# ✅ 최종 다항 로지스틱 회귀 실행
logit_model = sm.MNLogit(df_selected[target_variable], sm.add_constant(X_scaled_df))
logit_result = logit_model.fit()

# ✅ 회귀 결과 출력
print("\n📌 [다항 로지스틱 회귀 분석 결과]")
print(logit_result.summary())

# ✅ 예측 수행
y_pred_prob = logit_result.predict(sm.add_constant(X_scaled_df))
y_pred = y_pred_prob.idxmax(axis=1)

# ✅ 평가 지표 계산
accuracy = accuracy_score(df_selected[target_variable], y_pred)
precision = precision_score(df_selected[target_variable], y_pred, average="weighted", zero_division=0)
recall = recall_score(df_selected[target_variable], y_pred, average="weighted", zero_division=0)
f1 = f1_score(df_selected[target_variable], y_pred, average="weighted", zero_division=0)
roc_auc = roc_auc_score(pd.get_dummies(df_selected[target_variable]), y_pred_prob, multi_class="ovr")

# ✅ 평가 결과 출력
print("\n📌 [다항 로지스틱 회귀 평가 결과]")
print(f"  🔹 Accuracy (정확도): {accuracy:.4f}")
print(f"  🔹 Precision (정밀도): {precision:.4f}")
print(f"  🔹 Recall (재현율): {recall:.4f}")
print(f"  🔹 F1 Score: {f1:.4f}")
print(f"  🔹 ROC AUC Score: {roc_auc:.4f}")

# ✅ **분류 보고서 출력**
print("\n📌 [Classification Report]")
print(classification_report(df_selected[target_variable], y_pred))

print("\n✅ 다항 로지스틱 회귀 분석 완료!")
