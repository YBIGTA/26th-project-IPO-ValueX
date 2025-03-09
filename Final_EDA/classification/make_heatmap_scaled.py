import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

num = "three"
# ✅ feature_importance 폴더 내 모든 파일 리스트 가져오기
feature_importance_dir = f"feature_importance/{num}"
output_dir = f"plot/feature_importance/{num}/scaled"
os.makedirs(output_dir, exist_ok=True)  # ✅ 저장 폴더 생성
# ✅ "scaled" 폴더 경로 생성
scaled_dir = os.path.join(feature_importance_dir, "scaled")
os.makedirs(scaled_dir, exist_ok=True)  # 폴더 없으면 생성

files = sorted([f for f in os.listdir(feature_importance_dir) if f.startswith("classification_feature_importance_case_")])

# ✅ 히트맵 생성 함수 (LightGBM Scaling + 자동 저장)
def plot_feature_importance_heatmap(file_path, case_num):
    df = pd.read_csv(file_path, index_col=0).round(3)  # ✅ 소수점 3자리 반올림

    # 🔥 LightGBM의 Feature Importance를 합이 1이 되도록 스케일링
    if "LightGBM" in df.columns:
        df.loc[:, "LightGBM"] /= df["LightGBM"].sum()
        df = df.round(3)  # 🔥 다시 소수점 3자리 반올림

        # ✅ 스케일링된 데이터 저장
        scaled_file_path = os.path.join(scaled_dir, file.replace(".csv", "_scaled.csv"))
        df.to_csv(scaled_file_path, index=True)

    plt.rc('font', family='AppleGothic')
    plt.rcParams['axes.unicode_minus'] = False  # 마이너스(-) 기호 깨짐 방지

    fig_width = max(20, len(df) * 1.2)  # ✅ 기본 20인치 보장, feature 개수에 따라 자동 확장
    plt.figure(figsize=(fig_width, 10))  # 🔥 세로폭도 10으로 키움

    sns.heatmap(
        df.T, annot=True, fmt=".3f", cmap="coolwarm", linewidths=0.5,
        cbar_kws={'shrink': 0.5},  # 🔥 컬러바 크기 줄임
        annot_kws={"size": 12}  # 🔥 숫자 크기 조정 (더 잘 보이게)
    )
    
    plt.xlabel("Features", fontsize=16)
    plt.ylabel("Models", fontsize=16)
    plt.title(f"Feature Importance Heatmap (Case {case_num})", fontsize=18)
    plt.xticks(rotation=30, ha="right", fontsize=14)  # ✅ X축 글자 크기 증가 + 회전 조정
    plt.yticks(fontsize=14)  # ✅ Y축 글자 크기 증가

    # ✅ 저장
    output_path = os.path.join(output_dir, f"feature_importance_{num}_case_{case_num}_scaled.png")
    plt.savefig(output_path, dpi=300, bbox_inches="tight")  # 🔥 300 DPI 저장, bbox_inches="tight" 적용
    plt.close()  # ✅ 창 닫기 (메모리 절약)

# ✅ 모든 case에 대해 히트맵 생성 및 저장
for file in files:
    case_num = file.split("_")[-1].replace(".csv", "")
    file_path = os.path.join(feature_importance_dir, file)

    print(f"📌 Generating heatmap for Case {case_num}...")
    plot_feature_importance_heatmap(file_path, case_num)

print("\n✅ 모든 히트맵 저장 완료!")