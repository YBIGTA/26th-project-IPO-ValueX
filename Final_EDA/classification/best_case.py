import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from math import pi
num = "three"
# ✅ 폴더 생성
os.makedirs(f"plot/{num}/radar", exist_ok=True)
os.makedirs(f"plot/{num}/final", exist_ok=True)

# ✅ 한글 폰트 설정
plt.rc('font', family='AppleGothic')
plt.rcParams['axes.unicode_minus'] = False  # 마이너스(-) 기호 깨짐 방지

# ✅ metrics 폴더 내 모든 파일 가져오기
metrics_dir = f"metrics/{num}"
files = sorted([f for f in os.listdir(metrics_dir) if f.startswith("classification_metrics_case_")])

# ✅ 모든 case의 데이터를 저장할 딕셔너리
case_metrics = {}

# ✅ 모든 metrics 파일을 읽어와 딕셔너리에 저장
for file in files:
    case_num = file.split("_")[-1].replace(".csv", "")
    file_path = os.path.join(metrics_dir, file)

    df = pd.read_csv(file_path, index_col=0)
    case_metrics[case_num] = df

# ✅ 모든 데이터를 하나의 DataFrame으로 변환
df_all_metrics = pd.concat(case_metrics, names=["Case", "Model"])
df_all_metrics.reset_index(inplace=True)

# ✅ 숫자를 소수점 3자리로 반올림
df_all_metrics.iloc[:, 2:] = df_all_metrics.iloc[:, 2:].round(3)

# ✅ 1. Accuracy & F1-score 비교 (막대 그래프)
fig, axes = plt.subplots(1, 2, figsize=(15, 6))

# 🔹 Accuracy 비교
sns.barplot(data=df_all_metrics, x="Case", y="Accuracy", hue="Model", ax=axes[0])
axes[0].set_xticklabels(axes[0].get_xticklabels(), rotation=45)
axes[0].set_xlabel("Case Number")
axes[0].set_ylabel("Accuracy")
axes[0].set_title("Case별 모델별 Accuracy 비교")
axes[0].legend(loc="upper right")

# 🔹 F1-score 비교
sns.barplot(data=df_all_metrics, x="Case", y="F1-score", hue="Model", ax=axes[1])
axes[1].set_xticklabels(axes[1].get_xticklabels(), rotation=45)
axes[1].set_xlabel("Case Number")
axes[1].set_ylabel("F1-score")
axes[1].set_title("Case별 모델별 F1-score 비교")
axes[1].legend(loc="upper right")

plt.tight_layout()
plt.savefig(f"plot/{num}/final/accuracy_f1_comparison.png")  # ✅ 저장
plt.show()

# # ✅ 2. Best Case 찾기 (ROC AUC 기준)
# best_case = df_all_metrics.groupby("Case")["ROC AUC"].mean().idxmax()
# print(f"\n📌 Best Case (ROC AUC 기준): Case {best_case}")

# ✅ 3. 모든 Case에 대한 Radar Chart 생성
metrics_to_plot = ["Accuracy", "Precision", "Recall", "F1-score", "ROC AUC"]
if num == "three":
    metrics_to_plot = ["Accuracy", "Precision", "Recall", "F1-score"]

for case in df_all_metrics["Case"].unique():
    df_case = df_all_metrics[df_all_metrics["Case"] == case].groupby("Model")[metrics_to_plot].mean()

    labels = metrics_to_plot
    num_vars = len(labels)
    angles = np.linspace(0, 2 * np.pi, num_vars, endpoint=False).tolist()
    angles += angles[:1]  # Radar Chart 닫기

    plt.figure(figsize=(6, 6))
    for model, row in df_case.iterrows():
        values = row.tolist()
        values += values[:1]  # Radar Chart 닫기
        plt.polar(angles, values, label=model)

    plt.xticks(angles[:-1], labels, fontsize=12)
    plt.yticks(np.arange(0, 1.2, 0.2), color="grey", size=10)
    plt.ylim(0, 1)
    plt.title(f"Radar Chart - Case {case}")
    plt.legend(loc="upper right", bbox_to_anchor=(1.3, 1.1))

    plt.savefig(f"plot/{num}/radar/radar_chart_case_{case}.png")  # ✅ 저장
    plt.show()

# ✅ 4. Heatmap 시각화 (Accuracy & F1-score 기준)
fig, axes = plt.subplots(1, 2, figsize=(15, 6))

# 🔹 Accuracy Heatmap
df_pivot_acc = df_all_metrics.pivot_table(index="Case", columns="Model", values="Accuracy")
sns.heatmap(df_pivot_acc, annot=True, cmap="coolwarm", linewidths=0.5, ax=axes[0])
axes[0].set_xlabel("Model")
axes[0].set_ylabel("Case")
axes[0].set_title("Case별 모델 Accuracy Heatmap")

# 🔹 F1-score Heatmap
df_pivot_f1 = df_all_metrics.pivot_table(index="Case", columns="Model", values="F1-score")
sns.heatmap(df_pivot_f1, annot=True, cmap="coolwarm", linewidths=0.5, ax=axes[1])
axes[1].set_xlabel("Model")
axes[1].set_ylabel("Case")
axes[1].set_title("Case별 모델 F1-score Heatmap")

plt.tight_layout()
plt.savefig(f"plot/{num}/final/accuracy_f1_heatmap.png")  # ✅ 저장
plt.show()