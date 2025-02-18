import os
import pandas as pd

def split_csv_same_folder(file_path, split_index):
    # Load the CSV file
    df = pd.read_csv(file_path)

    # First part: rows from 2 to split_index (keeping column names)
    df_part1 = df.iloc[:split_index]

    # Second part: rows from split_index+1 to the end (keeping column names)
    df_part2 = df.iloc[split_index:]

    # Get the directory of the original file
    file_dir = os.path.dirname(file_path)

    # Define output file paths in the same directory
    part1_path = os.path.join(file_dir, "naver_stock_2023_1.csv")
    part2_path = os.path.join(file_dir, "naver_stock_2023_2.csv")

    # Save the first part
    df_part1.to_csv(part1_path, index=False)

    # Save the second part (keeping column headers)
    df_part2.to_csv(part2_path, index=False, header=True)

    return part1_path, part2_path

# 예제 실행 (파일 경로 설정 필요)
file_path = "C:/Users/kimhy/OneDrive/바탕 화면/NEWBIE_PROJECT/Crawler/38_crawler_for_share/database/Naver_Stock_2023.csv"  # 🔹 실제 CSV 파일 경로 입력
split_index = 6517  # 🔹 분할 기준 행

# 파일 분할 실행
split_file_1, split_file_2 = split_csv_same_folder(file_path, split_index)

print(f"파일 분할 완료:\n - {split_file_1}\n - {split_file_2}")
