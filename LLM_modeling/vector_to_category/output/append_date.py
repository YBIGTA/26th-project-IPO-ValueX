# import pandas as pd
# import os

# year_range = [i for i in range(2014, 2026)]

# for year in year_range:
#     cat_file_path = f"./cat_{year}.csv"
#     naver_stock_file_path = f"../../../Non_Finance_data/Naver_Stock/Processed_News/Naver_Stock_preprocessed_final_{year}.csv"

#     # Naver_Stock_preprocessed_final_{year}.csv 파일에서 link와 date 읽기
#     try:
#         naver_stock_df = pd.read_csv(naver_stock_file_path)
#         naver_stock_df = naver_stock_df[['Link', 'Date']]  # 필요한 컬럼만 추출
#         naver_stock_df = naver_stock_df.rename(columns={'Link': 'link'})  # 컬럼 이름 통일
#         naver_stock_df['date'] = pd.to_datetime(naver_stock_df['Date'], format='%Y%m%d').dt.strftime('%Y-%m-%d')
#         naver_stock_df = naver_stock_df[['link', 'date']]  # 순서 변경
#     except FileNotFoundError:
#         print(f"Error: {naver_stock_file_path} not found. Skipping year {year}.")
#         continue
#     except Exception as e:
#         print(f"Error reading {naver_stock_file_path}: {e}. Skipping year {year}.")
#         continue

#     # cat_{year}.csv 파일 읽기
#     try:
#         cat_df = pd.read_csv(cat_file_path)
#     except FileNotFoundError:
#         print(f"Error: {cat_file_path} not found. Skipping year {year}.")
#         continue
#     except Exception as e:
#         print(f"Error reading {cat_file_path}: {e}. Skipping year {year}.")
#         continue

#     # 두 DataFrame 병합
#     merged_df = pd.merge(naver_stock_df, cat_df, on='link', how='inner')

#     # 컬럼 순서 변경
#     merged_df = merged_df[['date', 'link', '성장_긍정', '성장_부정', '민감_긍정', '민감_부정', '방어_긍정', '방어_부정', '금융_긍정', '금융_부정']]

#     # cat_{year}.csv 파일 덮어쓰기
#     try:
#         merged_df.to_csv(cat_file_path, index=False)
#         print(f"{cat_file_path} updated successfully.")
#     except Exception as e:
#         print(f"Error writing to {cat_file_path}: {e}. Skipping year {year}.")
#         continue

# 이제 필요 없는 코드