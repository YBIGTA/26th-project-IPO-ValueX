import pandas as pd

csv_file_path = "../Finance_data/2nd기업명_상장일_산업군.csv"
output_file_path = "../Finance_data/섹터별분류.csv"

df = pd.read_csv(csv_file_path)

sector_mapping = {
    "Growth":['IT','소프트웨어','반도체','바이오','의료/의약','엔터','게임','로봇','배터리','전자/회로'],
    "Sensitive":['화학','기계제조','부품제조','자동차','금속','철강','선박','항공제조','자동차판매','건설','방산','에너지','투자'],
    "Protective":['식품','유통','옷','출판','가구/가전','서비스','스포츠','항공'],
    "Infra" : ['금융','출판'],

}

def assign_sector(industry):
    for sector, industries in sector_mapping.items():
        if industry in industries:
            return sector
        
    return "기타"


df['섹터'] = df['산업군'].apply(assign_sector)

df.to_csv(output_file_path,index=False , encoding="utf-8-sig")

