import json
import os
import pandas as pd
import optuna
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
import seaborn as sns

# ✅ 로그 및 결과 저장 폴더 생성
os.makedirs("log/two/best_params", exist_ok=True)
os.makedirs("feature_importance/two", exist_ok=True)
os.makedirs("metrics/two", exist_ok=True)

# ✅ VIF 계산 함수
def calculate_vif(X):
    vif_df = pd.DataFrame()
    vif_df["Feature"] = X.columns
    vif_df["VIF"] = [variance_inflation_factor(X.values, i) for i in range(X.shape[1])]
    return vif_df.sort_values(by="VIF", ascending=False)

# ✅ 케이스별 피처 선택 설정
def get_selected_blocks(case_num):
    cases = {
        1: ["상장정보"],
        2: ["상장정보", "재무정보"],  # Best
        "2-1": ["상장정보", "재무비율"],
        "2-2": ["상장정보", "재무변화율"],
        3: ["상장정보", "재무정보", "재무비율"],
        4: ["상장정보", "재무정보", "재무비율", "재무변화율"],
        5: ["상장정보", "재무정보", "재무비율", "재무변화율", "거시지표_vol"],
        6: ["상장정보", "재무정보", "재무비율", "재무변화율", "거시지표_change"],  # Best
        7: ["상장정보", "재무정보", "재무비율", "재무변화율", "거시지표_raw"],
        "5-2": ["상장정보", "재무정보", "거시지표_vol"],
        "6-2": ["상장정보", "재무정보", "거시지표_change"],
        "7-2": ["상장정보", "재무정보", "거시지표_raw"],
        8: ["상장정보", "재무정보", "재무비율", "거시지표_raw"],
        "9-1": ["상장정보", "재무정보", "재무비율", "재무변화율", "거시지표_raw", "산업군시장지수"],
        "9-2": ["상장정보", "재무정보", "재무비율", "거시지표_raw", "산업군시장지수"],
        10: ["상장정보", "재무정보", "재무비율", "재무변화율", "거시지표_raw", "산업군시장지수", "커뮤니티종목평가"],
        "10-2": ["상장정보", "재무정보", "재무비율", "거시지표_raw", "산업군시장지수", "커뮤니티종목평가"],
        11: ["상장정보", "재무정보", "재무비율", "재무변화율", "거시지표_raw", "산업군시장지수", "커뮤니티종목평가", "산업군별뉴스시황"],
        "11-1": ["상장정보", "재무정보", "재무비율", "거시지표_raw", "산업군시장지수", "커뮤니티종목평가", "산업군별뉴스시황"],
    }
    return cases.get(case_num, ["상장정보"])  # 기본값

# ✅ 데이터 로드 및 전처리
def load_and_preprocess_data(file_path, selected_blocks, target_variable, vif_threshold=20):
    with open(file_path, "r", encoding="utf-8") as f:
        block_data = json.load(f)

    df_blocks = {block: pd.DataFrame(entries) for block, entries in block_data.items()}
    df_fixed_panel = df_blocks["고정패널"].copy()
    df_fixed_panel.set_index(["기업명", "상장일", "산업군", "섹터"], inplace=True)

    df_selected = pd.concat([df_blocks[block] for block in selected_blocks], axis=1)
    df_target = df_blocks["종속변수"]
    df_selected[target_variable] = (df_target[target_variable] > 20).astype(int) # 기준설정

    df_fixed_panel_reset = df_fixed_panel.reset_index()
    df_selected = df_selected.merge(df_fixed_panel_reset[["기업명", "상장일", "산업군", "섹터"]], how="left", left_index=True, right_index=True)
    df_selected.set_index(["기업명", "상장일", "산업군", "섹터"], inplace=True)

    X = df_selected.drop(columns=[target_variable])
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    vif_df = calculate_vif(X)
    removed_vif_features = []
    while vif_df["VIF"].max() > vif_threshold and X.shape[1] > 1:
        remove_feature = vif_df.iloc[0]["Feature"]
        removed_vif_features.append(remove_feature)
        X = X.drop(columns=[remove_feature])
        X_scaled = scaler.fit_transform(X)
        vif_df = calculate_vif(X)

    return df_selected, X, df_selected[target_variable], vif_df, removed_vif_features
# ✅ 모델별 Optuna 최적화 수행
def optimize_model_hyperparameters(model_name, X_train, y_train):
    def objective(trial):
        if model_name == "RandomForest":
            model = RandomForestClassifier(
                n_estimators=trial.suggest_int("n_estimators", 50, 300, step=50),
                max_depth=trial.suggest_int("max_depth", 3, 15),
                min_samples_split=trial.suggest_int("min_samples_split", 2, 10),
                min_samples_leaf=trial.suggest_int("min_samples_leaf", 1, 5),
                random_state=42
            )

        elif model_name == "XGBoost":
            model = XGBClassifier(
                n_estimators=trial.suggest_int("n_estimators", 50, 300, step=50),
                max_depth=trial.suggest_int("max_depth", 3, 15),
                learning_rate=trial.suggest_loguniform("learning_rate", 0.01, 0.3),
                colsample_bytree=trial.suggest_float("colsample_bytree", 0.5, 1.0),
                random_state=42,
                use_label_encoder=False,
                eval_metric='logloss'
            )

        elif model_name == "LightGBM":
            model = LGBMClassifier(
                n_estimators=trial.suggest_int("n_estimators", 50, 300, step=50),
                max_depth=trial.suggest_int("max_depth", 3, 15),
                learning_rate=trial.suggest_loguniform("learning_rate", 0.01, 0.3),
                num_leaves=trial.suggest_int("num_leaves", 20, 150, step=10),
                random_state=42
            )

        elif model_name == "GradientBoosting":
            model = GradientBoostingClassifier(
                n_estimators=trial.suggest_int("n_estimators", 50, 300, step=50),
                max_depth=trial.suggest_int("max_depth", 3, 15),
                learning_rate=trial.suggest_loguniform("learning_rate", 0.01, 0.3),
                random_state=42
            )

        elif model_name == "LogisticRegression":
            model = LogisticRegression(
                C=trial.suggest_loguniform("C", 0.01, 10),
                max_iter=1000,
                random_state=42
            )

        model.fit(X_train, y_train)
        y_pred = model.predict(X_train)
        return f1_score(y_train, y_pred, average='macro')  # F1-Score 최적화

    study = optuna.create_study(direction="maximize")
    study.optimize(objective, n_trials=30)  # 30회 탐색

    return study.best_params  # 최적 파라미터 반환

# ✅ 모델 학습 및 평가
def train_and_evaluate_models(X_train, X_test, y_train, y_test, best_params, case_num):
    models = {
        "RandomForest": RandomForestClassifier(**best_params.get("RandomForest", {}), random_state=42),
        "XGBoost": XGBClassifier(**best_params.get("XGBoost", {}), use_label_encoder=False, eval_metric='logloss', random_state=42),
        "LightGBM": LGBMClassifier(**best_params.get("LightGBM", {}), random_state=42),
        "GradientBoosting": GradientBoostingClassifier(**best_params.get("GradientBoosting", {}), random_state=42),
        "LogisticRegression": LogisticRegression(**best_params.get("LogisticRegression", {}), max_iter=1000, random_state=42)
    }

    metrics_results = {}
    feature_importances = {}

    for name, model in models.items():
        model.fit(X_train, y_train)
        y_pred = model.predict(X_test)

        metrics_results[name] = {
            "Accuracy": accuracy_score(y_test, y_pred),
            "Precision": precision_score(y_test, y_pred, average='macro'),
            "Recall": recall_score(y_test, y_pred, average='macro'),
            "F1-score": f1_score(y_test, y_pred, average='macro'),
            "ROC AUC": roc_auc_score(y_test, model.predict_proba(X_test)[:, 1]) if hasattr(model, "predict_proba") else None
        }

        if hasattr(model, 'feature_importances_'):
            feature_importances[name] = model.feature_importances_
        else:
            feature_importances[name] = permutation_importance(model, X_test, y_test, n_repeats=10, random_state=42).importances_mean

    return metrics_results, feature_importances

# ✅ **MAIN 실행 함수**
def main():
    file_path = "classification_block_data_filled.json"
    case_nums = [1, 2, "2-1", "2-2", 3, 4, 5, 6, 7, "5-2", "6-2", "7-2", 8, "9-1", "9-2", 10, "10-2", 11, "11-1"]

    for case_num in case_nums:
        selected_blocks = get_selected_blocks(case_num)
        log_file_path = f"log/two/classification_results_case_{case_num}.txt"

        df_selected, X, y, vif_df, removed_vif_features = load_and_preprocess_data(file_path, selected_blocks, "종가대비등락율")

        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

        # ✅ 각 모델별 최적 파라미터 탐색
        best_params = {}
        for model_name in ["RandomForest", "XGBoost", "LightGBM", "GradientBoosting", "LogisticRegression"]:
            best_params[model_name] = optimize_model_hyperparameters(model_name, X_train, y_train)

            # ✅ 개별 모델 최적 파라미터 저장
            best_params_path = f"log/two/best_params/optuna_best_params_case_{case_num}.json"
            with open(best_params_path, "w", encoding="utf-8") as f:
                json.dump(best_params, f, indent=4)

        # ✅ 모델 학습 및 평가
        metrics_results, feature_importances = train_and_evaluate_models(X_train, X_test, y_train, y_test, best_params, case_num)

        # ✅ 결과 저장
        importance_df = pd.DataFrame(feature_importances, index=X.columns)
        metrics_df = pd.DataFrame(metrics_results).T

        importance_df.to_csv(f"feature_importance/two/classification_feature_importance_case_{case_num}.csv", index=True)
        metrics_df.to_csv(f"metrics/two/classification_metrics_case_{case_num}.csv", index=True)


        # ✅ 로그 파일 저장
        with open(log_file_path, "w", encoding="utf-8") as log_file:
            log_file.write(f"✅ Case {case_num} 실행 완료 | 사용 블록: {selected_blocks}\n\n")
            log_file.write("📌 [초기 다중공선성 (VIF) 분석 결과]\n")
            log_file.write(vif_df.to_string(index=False) + "\n\n")

            log_file.write("📌 [제거된 다중공선성 높은 변수 목록]\n")
            log_file.write("\n".join(removed_vif_features) + "\n\n")

            log_file.write("📌 [최종 다중공선성 (VIF) 분석 결과]\n")
            log_file.write(vif_df.to_string(index=False) + "\n")

    print("\n✅ 모든 케이스 실행 완료!")

# ✅ 실행
if __name__ == "__main__":
    main()