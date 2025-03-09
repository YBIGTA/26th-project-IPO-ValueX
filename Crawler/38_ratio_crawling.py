# import os
# import json
# import re
# import pandas as pd

# from datetime import datetime
# from selenium import webdriver
# from selenium.webdriver.chrome.options import Options
# from selenium.webdriver.common.by import By
# from selenium.webdriver.support.ui import WebDriverWait
# from selenium.webdriver.support import expected_conditions as EC
# from bs4 import BeautifulSoup
# from time import sleep
# import time
# import random
# from abc import ABC
# from typing import List, Optional, Dict

# class RatioCrawler(ABC):
#     def __init__(self, output_dir):
#         self.output_dir=output_dir
#         self.base_url= "https://www.38.co.kr/html/fund/index.htm?o=r1"
#         self.driver: Optional[webdriver.Chrome] = None
#         self.company_data: List[Dict] = []
#         self.not_found: List[str]=[]
#         self.lost_list: List[str]=[]
        
#     def start_browser(self, json_file:str):
#         try:
#             self.company_data=json.load(open(json_file,"r",encoding="utf-8-sig"))
#             chrome_options=Options()
#             self.driver=webdriver.Chrome(options=chrome_options)
#             self.driver.get(self.base_url)
#             print("38 커뮤니케이션 페이지 로딩 완료")
#         except:
#             print("38 커뮤니케이션 페이지 로딩 중 오류")
#             raise

#     def search_company(self, company: str):
#         search_box=self.driver.find_element(By.ID, "string")
#         search_keyword=company
#         search_box.send_keys(search_keyword)

#         search_button=self.driver.find_element(By.XPATH, "/html/body/table[3]/tbody/tr/td/table[1]/tbody/tr/td[1]/form/table/tbody/tr/td[4]/input")
#         search_button.click()

#         WebDriverWait(self.driver,3).until(EC.presence_of_all_elements_located((By.TAG_NAME, "a")))

#         soup=BeautifulSoup(self.driver.page_source,"html.parser")

#         table=soup.find("table",attrs={"summary":"수요예측결과"})

#         try:
#             row=table.find("a", text=lambda text: text and company in text).find_parent("tr")
#         except:
#             print(f"{company} Not found.")
#             self.not_found.append(company)
#             return None
        
#         ratio=None
#         column=row.find_all("td")[6].get_text(strip=True)

#         if column=="-":
#             ratio="0 %"
#         else:
#             ratio=column
#         self.driver.get(self.base_url)
#         return ratio
    
#     def get_company_name(self):
#         companies=[]
#         for company in self.company_data:
#             name=list(company.keys())[0]
#             if company[name]["수요예측"]["의무보유확약비율"]=="":
#                 companies.append(name)
#         self.lost_list=companies
#         print(self.lost_list)
    
#     def crawl(self):
#         for lost_company in self.lost_list:
#             for company in self.company_data:
#                 company_name=list(company.keys())[0]
#                 if lost_company==company_name:
#                     try:
#                         ratio=self.search_company(company_name)
#                         company[company_name]["수요예측"]["의무보유확약비율"]=ratio
#                         print(f"{lost_company} 의무보유확약비율 수정완료")
#                     except:
#                         print(f"{lost_company} 검색결과 없음, 업데이트 스킵")
#                         self.not_found.append(lost_company)

#     def save(self):
#         with open("./Finance_data/IPOSTOCK_data.json", "w", encoding="utf-8-sig") as f:
#             json.dump(self.company_data, f, ensure_ascii=False, indent=4)

#         print("✅ JSON 파일이 성공적으로 업데이트되었습니다.")

#         self.driver.quit()

# if __name__=="__main__":
#     output_directory="./Finance_data/"

#     crawler=RatioCrawler(output_dir=output_directory)
#     crawler.start_browser("./Finance_data/IPOSTOCK_data.json")
#     crawler.get_company_name()
#     crawler.crawl()
#     crawler.save()

import os
import json
import re
import pandas as pd

from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
from time import sleep
import time
import random
from abc import ABC
from typing import List, Optional, Dict, Any

class RatioCrawler(ABC):
    def __init__(self, json_file):
        self.json_file = json_file
        self.base_url = "https://www.38.co.kr/html/fund/index.htm?o=r1"
        self.driver: Optional[webdriver.Chrome] = None
        self.company_data: List[Dict[str, Any]] = []  # JSON 데이터를 리스트로 관리
        self.not_found: List[str] = []
        self.lost_list: List[str] = []

    def start_browser(self):
        """ Selenium을 이용해 브라우저를 시작하고 38 커뮤니케이션 페이지를 로딩 """
        try:
            with open(self.json_file, "r", encoding="utf-8-sig") as f:
                self.company_data = json.load(f)  # JSON 데이터를 리스트로 로드
            
            chrome_options = Options()
            self.driver = webdriver.Chrome(options=chrome_options)
            self.driver.get(self.base_url)
            print("✅ 38 커뮤니케이션 페이지 로딩 완료")
        except Exception as e:
            print("❌ 38 커뮤니케이션 페이지 로딩 중 오류:", str(e))
            raise

    def search_company(self, company: str):
        """ 특정 기업의 의무보유확약비율을 찾음 """
        try:
            # 검색창 찾기
            search_box = self.driver.find_element(By.ID, "string")
            search_box.clear()  # 이전 검색 내용 초기화
            search_box.send_keys(company)

            # 검색 버튼 클릭
            search_button = self.driver.find_element(By.XPATH, "/html/body/table[3]/tbody/tr/td/table[1]/tbody/tr/td[1]/form/table/tbody/tr/td[4]/input")
            search_button.click()

            # 검색 결과 로딩 대기
            WebDriverWait(self.driver, 3).until(EC.presence_of_all_elements_located((By.TAG_NAME, "a")))

            # 페이지에서 데이터 파싱
            soup = BeautifulSoup(self.driver.page_source, "html.parser")
            table = soup.find("table", attrs={"summary": "수요예측결과"})

            # 테이블이 없는 경우 예외 처리
            if not table:
                print(f"❌ {company} - 수요예측결과 테이블 없음")
                self.not_found.append(company)
                return None

            # 기업명이 포함된 <a> 태그 찾기
            link = table.find("a", text=lambda text: text and company in text)
            if not link:
                print(f"❌ {company} - 기업명을 찾을 수 없음")
                self.not_found.append(company)
                return None

            # 해당 <tr> 찾기
            row = link.find_parent("tr")
            if not row:
                print(f"❌ {company} - 해당 행(tr) 없음")
                self.not_found.append(company)
                return None

            # 의무보유확약비율 값 가져오기
            columns = row.find_all("td")
            if len(columns) < 7:
                print(f"❌ {company} - 의무보유확약비율 데이터 부족")
                self.not_found.append(company)
                return None

            ratio = columns[6].get_text(strip=True)  # 7번째 컬럼 (index 6)
            if ratio == "-":
                ratio = "0 %"

            print(f"✅ {company} - 의무보유확약비율: {ratio}")

            # 검색 후 원래 페이지로 복귀
            self.driver.get(self.base_url)
            return ratio
        except Exception as e:
            print(f"❌ {company} 검색 중 오류 발생: {str(e)}")
            self.not_found.append(company)
            return None

    def get_company_name(self):
        """ 의무보유확약비율이 빈 값이거나 None인 기업 리스트 생성 """
        companies = []
        for company in self.company_data:
            company_name = list(company.keys())[0]  # 기업명 가져오기
            if not company[company_name]["수요예측"].get("의무보유확약비율"):  # 빈 값 또는 None 체크
                companies.append(company_name)
        self.lost_list = companies
        print(f"🔍 검색해야 할 기업 수: {len(self.lost_list)}")

    def crawl(self):
        """ 의무보유확약비율이 없는 기업을 검색하여 업데이트 """
        for lost_company in self.lost_list:
            for company in self.company_data:
                company_name = list(company.keys())[0]
                if lost_company == company_name:
                    try:
                        ratio = self.search_company(company_name)
                        if ratio is not None:
                            company[company_name]["수요예측"]["의무보유확약비율"] = ratio
                            print(f"✅ {lost_company} 의무보유확약비율 수정 완료")
                        else:
                            print(f"❌ {lost_company} 검색 결과 없음, 업데이트 스킵")
                            self.not_found.append(lost_company)
                    except Exception as e:
                        print(f"❌ {lost_company} 검색 중 오류 발생: {str(e)}")
                        self.not_found.append(lost_company)

    def save(self):
        """ JSON 데이터를 저장 """
        with open(self.json_file, "w", encoding="utf-8-sig") as f:
            json.dump(self.company_data, f, ensure_ascii=False, indent=4)

        print("✅ JSON 파일이 성공적으로 업데이트되었습니다.")

        self.driver.quit()


# 실행
if __name__ == "__main__":
    json_file_path = "./Finance_data/IPOSTOCK_data.json"

    crawler = RatioCrawler(json_file=json_file_path)
    crawler.start_browser()
    crawler.get_company_name()
    crawler.crawl()
    crawler.save()
