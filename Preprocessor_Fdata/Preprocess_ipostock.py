import pandas as pd
import json
from datetime import datetime
import re

def run_process_ipostock(input_json="IPOSTOCK_data.json", output_csv="X_stat.csv"):
    with open(input_json, "r", encoding="utf-8-sig") as file:
        json_data = json.load(file)

    data_list = []
    
    for company_data in json_data:
        for company_name, details in company_data.items():
            date_str = details.get("공모정보", {}).get("상장일", None)
            if date_str:
                full_date = datetime.strptime(date_str, "%Y.%m.%d").strftime("%Y-%m-%d")
                year_key = full_date[:4]
                month_key = full_date[:7]

                row = {
                    "기업명": company_name,
                    "year_key": year_key,
                    "month_key": month_key,
                    "상장일": full_date,
                    "공모후 발행주식수": int(details.get("주주구성", {}).get("공모후 발행주식수", "0").replace(",", "")),
                    "청약경쟁률": re.findall(r"\d+", details.get("공모정보", {}).get("청약경쟁률", "0"))[0],
                }

                data_list.append(row)

    df = pd.DataFrame(data_list)
    # df.to_csv(output_csv, index=False, encoding="utf-8-sig")
    print(f"✅ {output_csv} 생성 완료!")

    return df

# if __name__ == "__main__":
#     run_process_ipostock()