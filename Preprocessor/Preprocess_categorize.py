import pandas as pd

csv_file_path = "../Finance_data/2nd기업명_상장일_산업군.csv"

df = pd.read_csv(csv_file_path)

industry_counts = df['산업군'].fillna("빈칸").value_counts().reset_index()
industry_counts.columns = ['산업군' , '빈도수']

print(industry_counts)

# 📌 '산업군'이 빈칸인 기업명과 상장일 출력
null_entries = df[df['산업군'].isna()][['기업명', '상장일']]
print(null_entries)
