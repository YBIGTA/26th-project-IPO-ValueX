import pandas as pd
import os

def load_regression_data():
    base_path = os.path.abspath(os.path.dirname(__file__))
    regression_file_path = os.path.join(base_path, "df_regression.csv")
    
    df = pd.read_csv(regression_file_path)
    return df

def prepare_regression_input(df):
    # 제거할 컬럼 목록 (최대주주 보호예수 기간은 유지)
    drop_columns = ["기업명", "year_key", "month_key", "상장일"]
    df = df.drop(columns=drop_columns)
    return df

def save_regression_input(df):
    output_file = "regression_input.csv"
    df.to_csv(output_file, index=False)
    print(f"파일이 저장되었습니다: {output_file}")

def main():
    df = load_regression_data()
    df_regression_input = prepare_regression_input(df)
    save_regression_input(df_regression_input)

if __name__ == "__main__":
    main()







