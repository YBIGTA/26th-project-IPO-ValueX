import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import accuracy_score, confusion_matrix, classification_report

# 데이터 로드
df = pd.read_csv("regression_input.csv")

# 종속변수와 독립변수 설정
X = df.drop(columns=["종가대비등락율"])
y = df["종가대비등락율"]

# 학습 데이터 분할
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# 회귀 모델 학습
model = RandomForestRegressor(n_estimators=100, random_state=42)
model.fit(X_train, y_train)

# 예측
y_pred = model.predict(X_test)

# 예측값을 구획으로 변환
def categorize_return(y):
    if y <= 0:
        return 0
    elif y <= 60:
        return 1
    else:
        return 2

y_pred_class = np.array([categorize_return(val) for val in y_pred])
y_test_class = np.array([categorize_return(val) for val in y_test])

# 평가
accuracy = accuracy_score(y_test_class, y_pred_class)
conf_matrix = confusion_matrix(y_test_class, y_pred_class)
class_report = classification_report(y_test_class, y_pred_class)

print(f"정확도: {accuracy:.4f}")
print("혼동 행렬:\n", conf_matrix)
print("분류 리포트:\n", class_report)
