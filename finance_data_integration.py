# import os
# import json
# import re
# import pandas as pd

# from datetime import datetime

# base_dir = os.path.dirname(__file__)
# finance_data_dir=os.path.join(base_dir,"Finance_data")
# ipostock_path=os.path.join(finance_data_dir,"IPOSTOCK_data.json")
# treasury_3_path=os.path.join(finance_data_dir, "KOREA_국고채_금리_(3년).json")
# treasury_10_path=os.path.join(finance_data_dir, "KOREA_국고채_금리_(10년).json")
# ipostock_data=json.load(open(ipostock_path,mode='r', encoding='utf-8'))
# treasury_3_data=json.load(open(treasury_3_path,mode='r',encoding='utf-8'))
# treasury_10_data=json.load(open(treasury_10_path,mode='r',encoding='utf-8'))

# output_dir=os.path.join(finance_data_dir,"finance_data.csv")

# def extract_int(text):
#     """문자열에서 쉼표가 있든 없든 숫자만 추출하여 int로 변환"""
#     match = re.search(r"\d+(?:,\d+)*", text)  # 쉼표가 포함된 숫자 또는 일반 숫자 찾기
#     if match:
#         return int(match.group().replace(",", ""))  # 쉼표 제거 후 정수 변환
#     return None  # 숫자가 없을 경우 None 반환

# def extract_float(text):
#     """문자열에서 쉼표가 있든 없든 숫자만 추출하여 int로 변환"""
#     match = re.search(r"\d+(?:,\d+)*", text)  # 쉼표가 포함된 숫자 또는 일반 숫자 찾기
#     if match:
#         return float(match.group().replace(",", ""))  # 쉼표 제거 후 정수 변환
#     return None  # 숫자가 없을 경우 None 반환

# for company in ipostock_data:
#     # 기업명 - name
#     name=list(company.keys())[0]

#     # 상장일 - date
#     date=datetime.strptime(company[name]["공모정보"]["상장일"],"%Y.%m.%d").date()
    
#     # |희망가격하한가-확정공모가| - low_price
#     boundaries=re.findall(r"\d+(?:,\d+)*",company[name]["수요예측"]["(희망)공모가격"])
#     low_boudary=int(boundaries[0].replace(",",""))
#     confirmed_price=extract_int(company[name]["공모정보"]["(확정)공모가격"])
#     low_price=abs(low_boudary-confirmed_price)

#     # |희망가격상한가-확정공모가| - high_price
#     high_boundary=int(boundaries[1].replace(",",""))
#     high_price=abs(high_boundary-confirmed_price)

#     # 의무보유확약비율 - commitment_ratio
#     commitment_ratio=extract_float(company[name]["수요예측"]["의무보유확약비율"])

#     # 청약경쟁률 - competition_rate
#     competition_rate=extract_int(company[name]["공모정보"]["청약경쟁률"])

#     # 공모후 발행주식수 - after_offer
#     after_offer=extract_int(company[name]["주주구성"]["공모후 발행주식수"])

#     # 최대주주 소유주 비율 - largest_shareholder_percentage
#     largest_shareholder_percentage=extract_float(company[name]["주주구성"]["주주구성 table"]["보호예수매도금지"][list(company[name]["주주구성"]["주주구성 table"]["보호예수매도금지"].keys())[0]][0])

#     # 최대주주 보호예수기간 - largest_shareholder_period  (단위: 월)
#     largest_shareholder_period=extract_int(company[name]["주주구성"]["주주구성 table"]["보호예수매도금지"][list(company[name]["주주구성"]["주주구성 table"]["보호예수매도금지"].keys())[0]][2])
    
#     # 보호예수물량합계 비율 - total_protection_deposit_percentage
#     total_protection_deposit_percentage=extract_float(company[name]["주주구성"]["주주구성 table"]["보호예수매도금지"][list(company[name]["주주구성"]["주주구성 table"]["보호예수매도금지"].keys())[0]][3])

#     # 부채비율 = 부채총계/자본총계 - debt_percentage
#     debt_percentage=company[name]["재무정보"]["부채총계"][0]/company[name]["재무정보"]["자본총계"][0]

#     # 유동비율 = 유동자산/유동부채 - liquid_percentage
#     liquid_percentage=company[name]["재무정보"]["유동자산"][0]/company[name]["재무정보"]["유동부채"][0]

#     # 영업이익률(가중평균) = 영업이익/매출액 의 3개년 가중평균 - business_profit
#     # business_profit1이 가장 최근
#     business_profit1=company[name]["재무정보"]["영업이익"][0]/company[name]["재무정보"]["매출액"][0]
#     w1=0.5
#     business_profit2=company[name]["재무정보"]["영업이익"][1]/company[name]["재무정보"]["매출액"][1]
#     w2=0.33
#     business_profit3=company[name]["재무정보"]["영업이익"][2]/company[name]["재무정보"]["매출액"][2]
#     w3=0.17

#     business_profit=business_profit1*w1+business_profit2*w2+business_profit3*w3

#     # 당기순이익률(가중평균) = 당기순이익/매출액 - net_profit
    # try:
    #     net_profit1=company[name]["재무정보"]["당기순이익"][0]/company[name]["재무정보"]["매출액"][0]
    #     w1=0.5
    #     net_profit2=company[name]["재무정보"]["당기순이익"][1]/company[name]["재무정보"]["매출액"][1]
    #     w2=0.33
    #     net_profit3=company[name]["재무정보"]["당기순이익"][2]/company[name]["재무정보"]["매출액"][2]
    #     w3=0.17
    # except:
    #     net_profit1=company[name]["재무정보"]["당기순이익"][0]/company[name]["재무정보"]["영업수익"][0]
    #     w1=0.5
    #     net_profit2=company[name]["재무정보"]["당기순이익"][1]/company[name]["재무정보"]["영업수익"][1]
    #     w2=0.33
    #     net_profit3=company[name]["재무정보"]["당기순이익"][2]/company[name]["재무정보"]["영업수익"][2]
    #     w3=0.17
    # net_profit=net_profit1*w1+net_profit2*w2+net_profit3*w3

#     # 매출액 성장률(가중평균) = (올해 매출 - 작년매출)  / 작년매출 - profit_growth
#     profit1=company[name]["재무정보"]["매출액"][0]
#     profit2=company[name]["재무정보"]["매출액"][1]
#     profit3=company[name]["재무정보"]["매출액"][2]

#     profit_growth1=(profit1-profit2)/profit2
#     w1=0.67
#     profit_growth2=(profit2-profit3)/profit3
#     w2=0.33
    
#     profit_growth=profit_growth1*w1+profit_growth2*w2

#     # 영업이익 성장률(가중평균) = (올해 영업이익 - 작년 영업이익) / 작년 영업이익 - business_profit_growth
#     business_profit1=company[name]["재무정보"]["영업이익"][0]
#     business_profit2=company[name]["재무정보"]["영업이익"][1]
#     business_profit3=company[name]["재무정보"]["영업이익"][2]

#     business_profit_growth1=(business_profit1-business_profit2)/business_profit2
#     w1=0.67
#     business_profit_growth2=(business_profit2-business_profit3)/business_profit3
#     w2=0.33
    
#     business_profit_growth=business_profit_growth1*w1+business_profit_growth2*w2

#     # ROE(가중평균) = (당기순이익 / 자본총계)*100 - roe
#     roe1=company[name]["재무정보"]["당기순이익"][0]*100/company[name]["재무정보"]["자본총계"][0]
#     w1=0.5
#     roe2=company[name]["재무정보"]["당기순이익"][1]*100/company[name]["재무정보"]["자본총계"][1]
#     w2=0.33
#     roe3=company[name]["재무정보"]["당기순이익"][2]*100/company[name]["재무정보"]["자본총계"][2]
#     w3=0.17

#     roe=roe1*w1+roe2*w2+roe3*w3

#     # EPS(가중평균) = 당기순이익 / 공모후 발행주식수 - eps
#     eps1=company[name]["재무정보"]["당기순이익"][0]/extract_int(company[name]["주주구성"]["공모후 발행주식수"][0])
#     w1=0.5
#     eps2=company[name]["재무정보"]["당기순이익"][1]/extract_int(company[name]["주주구성"]["공모후 발행주식수"][0])
#     w2=0.33
#     eps3=company[name]["재무정보"]["당기순이익"][2]/extract_int(company[name]["주주구성"]["공모후 발행주식수"][0])
#     w3=0.17

#     eps=eps1*w1+eps2*w2+eps3*w3

#     # EV/영업이익 = (자산총계 - 부채총계) / 영업이익 - ev
#     ev=(company[name]["재무정보"]["자산총계"][0]-company[name]["재무정보"]["부채총계"][0])/company[name]["재무정보"]["영업이익"][0]


#     # 기업별 data 저장
#     company_data=[name, date, low_price,high_price,commitment_ratio, competition_rate, after_offer, largest_shareholder_percentage,largest_shareholder_period, total_protection_deposit_percentage, 
#                   debt_percentage, liquid_percentage, business_profit, net_profit, profit_growth, business_profit_growth, roe, eps, ev]
    
#     df=pd.read_csv(output_dir)

#     company_data_extended=company_data+[None]*(len(df.columns)-len(company_data))

#     new_df=pd.DataFrame([company_data_extended],columns=df.columns)

#     df=pd.concat([df,new_df],ignore_index=True )
#     df.to_csv(output_dir,index=False)
    




#     # 국고채금리 스프레드 - treasury_spread  
#     # BSI - bsi
#     # 기준금리 - base_interest
#     # 무역수지 - trade_balance
#     # IPI - ipi
#     # PPI - ppi
#     # CSI - csi
#     # CPI - cpi
#     # 수출입물량지수 - export_import_volume
#     # 외환보유액 - foreign_exchange
#     # 원달러환율 - exchange_rate
#     # 통화량(M2) - m2
#     # KOSPI1001
#     # KOSPI1005
#     # KOSPI1008
#     # KOSPI1011
#     # KOSPI1012
#     # KOSPI1013
#     # KOSPI1014
#     # KOSPI1015
#     # KOSPI1017
#     # KOSPI1018
#     # KOSPI1020
#     # KOSPI1021
#     # KOSPI1024
#     # KOSPI1026
#     # KOSPI1035
#     # KOSPI1155
#     # KOSPI1156


import os
import json
import re
import pandas as pd
from datetime import datetime

base_dir = os.path.dirname(__file__)
finance_data_dir = os.path.join(base_dir, "Finance_data")
ipostock_path = os.path.join(finance_data_dir, "IPOSTOCK_data.json")
output_dir = os.path.join(finance_data_dir, "finance_data.csv")

ipostock_data = json.load(open(ipostock_path, mode='r', encoding='utf-8-sig'))

def extract_int(text):
    """문자열에서 쉼표가 있든 없든 숫자만 추출하여 int로 변환"""
    match = re.search(r"\d+(?:,\d+)*", text)
    if match:
        return int(match.group().replace(",", ""))
    return None

def extract_float(text):
    """문자열에서 쉼표가 있든 없든 숫자만 추출하여 float로 변환"""
    match = re.search(r"\d+(?:,\d+)*", text)
    if match:
        return float(match.group().replace(",", ""))
    return None

for company in ipostock_data:
    name = list(company.keys())[0]  # 기업명
    
    print(f"\n▶ Processing company: {name}")

    # 상장일
    date_str = company[name]["공모정보"]["상장일"]
    date = datetime.strptime(date_str, "%Y.%m.%d").date()
    print(f"  상장일: {date}")

    # 희망가격 범위 파싱
    boundaries = re.findall(r"\d+(?:,\d+)*", company[name]["수요예측"]["(희망)공모가격"])
    low_boundary = int(boundaries[0].replace(",", ""))
    high_boundary = int(boundaries[1].replace(",", ""))
    confirmed_price = extract_int(company[name]["공모정보"]["(확정)공모가격"])
    low_price = abs(low_boundary - confirmed_price)
    high_price = abs(high_boundary - confirmed_price)
    
    print(f"  희망가격 하한가: {low_boundary}, 상한가: {high_boundary}, 확정공모가: {confirmed_price}")
    print(f"  |하한가-확정공모가|: {low_price}, |상한가-확정공모가|: {high_price}")

    # 의무보유확약비율
    commitment_ratio = extract_float(company[name]["수요예측"]["의무보유확약비율"])
    competition_rate = extract_int(company[name]["공모정보"]["청약경쟁률"])
    after_offer = extract_int(company[name]["주주구성"]["공모후 발행주식수"])

    print(f"  의무보유확약비율: {commitment_ratio}, 청약경쟁률: {competition_rate}, 공모후 발행주식수: {after_offer}")

    # 최대주주 정보
    largest_shareholder_info = list(company[name]["주주구성"]["주주구성 table"]["보호예수매도금지"].values())[0]
    largest_shareholder_percentage = extract_float(largest_shareholder_info[0])
    largest_shareholder_period = extract_int(largest_shareholder_info[2])
    total_protection_deposit_percentage = extract_float(list(company[name]["주주구성"]["주주구성 table"]["보호예수매도금지"].values())[-1][2])

    print(f"  최대주주 소유주 비율: {largest_shareholder_percentage}, 보호예수기간: {largest_shareholder_period}, 보호예수비율: {total_protection_deposit_percentage}")

    # 부채비율 및 유동비율
    try:
        debt_percentage = company[name]["재무정보"]["부채총계"][0] / company[name]["재무정보"]["자본총계"][0]
    except (ZeroDivisionError, KeyError, TypeError):
        debt_percentage = "-"

    try:
        liquid_percentage = company[name]["재무정보"]["유동자산"][0] / company[name]["재무정보"]["유동부채"][0]
    except (ZeroDivisionError, KeyError, TypeError):
        liquid_percentage = "-"

    print(f"  부채비율: {debt_percentage}, 유동비율: {liquid_percentage}")

    revenue_key = "매출액" if "매출액" in company[name]["재무정보"] else "영업수익"

    # 영업이익률(가중평균)
    try:
        business_profit = sum([
            (company[name]["재무정보"]["영업이익"][i] / company[name]["재무정보"][revenue_key][i] * w)
            for i, w in enumerate([0.5, 0.33, 0.17])
        ])
    except (ZeroDivisionError, KeyError, TypeError):
        business_profit = "-"

    print(f"  영업이익률(가중평균): {business_profit}")

    # 당기순이익률(가중평균) = 당기순이익/매출액 - net_profit
    try:
        net_profit = sum([
            (company[name]["재무정보"]["당기순이익"][i] / company[name]["재무정보"][revenue_key][i] * w)
            for i, w in enumerate([0.5, 0.33, 0.17])
        ])
    except (ZeroDivisionError, KeyError, TypeError):
        net_profit = "-"

    print(f" 당기순이익률(가중평균): {net_profit}")

    # 매출액 성장률(가중평균) = (올해 매출 - 작년매출) / 작년매출 - profit_growth
    try:
        profit_growth = sum([
            ((company[name]["재무정보"][revenue_key][i] - company[name]["재무정보"][revenue_key][i+1]) / company[name]["재무정보"][revenue_key][i+1] * w)
            for i, w in enumerate([0.67, 0.33])
        ])
    except (ZeroDivisionError, KeyError, TypeError):
        profit_growth = "-"
    print(f" 매출액 성장률(가중평균): {profit_growth}")

    # 영업이익 성장률(가중평균) = (올해 영업이익 - 작년 영업이익) / 작년 영업이익 - business_profit_growth
    try:
        business_profit_growth = sum([
            ((company[name]["재무정보"]["영업이익"][i] - company[name]["재무정보"]["영업이익"][i+1]) / company[name]["재무정보"]["영업이익"][i+1] * w)
            for i, w in enumerate([0.67, 0.33])
        ])
    except (ZeroDivisionError, KeyError, TypeError):
        business_profit_growth = "-"
    print(f" 영업이익 성장률(가중평균): {business_profit_growth}")

    # ROE(가중평균) = 당기순이익 / 공모후 발행주식수 - roe
    try:
        roe = sum([
            (company[name]["재무정보"]["당기순이익"][i] / company[name]["재무정보"]["자본총계"][i] * w)
            for i, w in enumerate([0.5, 0.33, 0.17])
        ])
    except (ZeroDivisionError, KeyError, TypeError):
        roe = "-"
    print(f" ROE(가중평균): {roe}")

    # EPS (가중평균)
    try:
        eps = sum([
            company[name]["재무정보"]["당기순이익"][i] / extract_int(company[name]["주주구성"]["공모후 발행주식수"])
            for i, w in enumerate([0.5, 0.33, 0.17])
        ])
    except (ZeroDivisionError, KeyError, TypeError):
        eps = "-"
    print(f"  EPS(가중평균): {eps}")

    # EV/영업이익 = (자산총계 - 부채총계) / 영업이익 -ev
    try:
        ev=(company[name]["재무정보"]["자산총계"][0]-company[name]["재무정보"]["부채총계"][0])/company[name]["재무정보"]["영업이익"][0]
    except (ZeroDivisionError, KeyError, TypeError):
        ev = "-"
    print(f" EV : {ev}")

    # 종가대비등락율 - earning_rate
    earning_rate = company[name]["종가대비등락율"]
    print(f" 종가대비등락율(종속변수): {earning_rate}")

    

    # 데이터 딕셔너리 생성
    company_data = {
        "name": name,
        "date": date,
        "low_price": low_price,
        "high_price": high_price,
        "commitment_ratio": commitment_ratio,
        "competition_rate": competition_rate,
        "after_offer": after_offer,
        "largest_shareholder_percentage": largest_shareholder_percentage,
        "largest_shareholder_period": largest_shareholder_period,
        "total_protection_deposit_percentage": total_protection_deposit_percentage,
        "debt_percentage": debt_percentage,
        "liquid_percentage": liquid_percentage,
        "business_profit": business_profit,
        "net_profit": net_profit,
        "profit_growth": profit_growth,
        "business_profit_growth": business_profit_growth,
        "roe": roe,
        "eps": eps,
        "ev" : ev,
        "earning_rate": earning_rate
    }

    # CSV 읽기 및 데이터 추가
    df = pd.read_csv(output_dir, encoding='utf-8-sig')
    print(f"  기존 CSV 데이터 크기: {df.shape}")

    # CSV 컬럼 순서를 유지하면서 매칭
    new_row = {col: company_data.get(col, None) for col in df.columns}

    # 데이터프레임에 추가
    df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)

    df.to_csv(output_dir, index=False, encoding='utf-8-sig')

    print(f"  데이터 추가 완료! 현재 CSV 크기: {df.shape}")

