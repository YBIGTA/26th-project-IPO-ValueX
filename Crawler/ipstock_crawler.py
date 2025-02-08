import os
import json


from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
from time import sleep
import time
from abc import ABC
from typing import List, Optional, Dict

def load_company_names(json_file):
    with open(json_file,"r",encoding="utf-8") as f:
        data=json.load(f)
    return [company["기업명"] for company in data]

class IpostockCrawler(ABC):
    def __init__(self, output_dir: str):
        self.output_dir = output_dir
        self.base_url: str = 'http://www.ipostock.co.kr/sub03/ipo08.asp?str4=2024&str5=all'
        self.datas: List[Dict] = []
        self.driver: Optional[webdriver.Chrome] = None
        self.company_list:List[str] = load_company_names("Finance_data/KIND_data.json")

    def start_browser(self):
        try:
            chrome_options=Options()
            self.driver=webdriver.Chrome(options=chrome_options)
            self.driver.get(self.base_url)
            self.driver.implicitly_wait(20)
            print("ipostock 페이지 로딩 완료")
        except Exception as e:
            print("ipostock 페이지 로딩 중 오류")
            raise

    def search_company(self, company:str):
        search_box = self.driver.find_element(By.CLASS_NAME, "FORM1")  # 검색창 (name="str3")
        search_keyword = company
        search_box.send_keys(search_keyword)

        search_button=self.driver.find_element(By.XPATH, "//input[@type='image' and contains(@src, 'btn_search.gif')]")
        search_button.click()
        time.sleep(3)

        soup=BeautifulSoup(self.driver.page_source,"html.parser")

        for a in soup.find_all("a"):
            found_text=a.find("font").get_text(strip=True).rstrip(".") if a.find("font") else a.get_text(strip=True).rstrip(".")

            if not found_text:
                continue
            if company.startswith(found_text):
                company_url=a.get("href")
                full_url=f"http://www.ipostock.co.kr{company_url}"

                self.driver.get(full_url)
                time.sleep(2)
                print(f"\n🔍 {company} 검색 완료\n")
                return

        

        # try:
        #     result = self.driver.find_element(By.XPATH, f"//a[contains(text(), '{company}')]")
        #     print("검색결과 존재")
        #     result.click()
        #     print(f"{company} 검색 후 클릭 완료.")
            
        # except Exception as e:
        #     print(f"{company}검색과정에서 오류")
        #     raise
    

    def scrape_data(self):
        """ 기업별 데이터 크롤링 """
        result_data = []

        for company in self.company_list:
            self.search_company(company)

            time.sleep(2)
            soup = BeautifulSoup(self.driver.page_source, "html.parser")

            def get_data(label):
                """ 특정 라벨에 해당하는 데이터를 가져옴 """
                for td in soup.find_all("td"):
                    if td.get_text(strip=True) == label:  # 태그 내부 텍스트 가져와 비교
                        next_td = td.find_next_sibling("td")
                        return next_td.get_text(strip=True) if next_td else None
                return None
            
            # 수요예측 탭 크롤링
            wanted_ipo_price = get_data("(희망)공모가격")
            competition_rate = get_data("단순 기관경쟁률")
            lockup_ratio = get_data("의무보유확약비율")
            print(f"✅ {company} 수요예측 tab 크롤링 완료")
            print(f"   ├─ (희망) 공모가격: {wanted_ipo_price}")
            print(f"   ├─ 단순 기관경쟁률: {competition_rate}")
            print(f"   └─ 의무보유확약비율: {lockup_ratio}\n")

            # 공모정보 탭으로 이동
            try:
                offering_info_btn = self.driver.find_element(By.XPATH, "//*[@id='print']/table/tbody/tr[5]/td/table[1]/tbody/tr[1]/td[4]/a")
                offering_info_btn.click()
                time.sleep(2)
                print(f"✅ {company} 공모정보 tab 클릭 완료")
                soup = BeautifulSoup(self.driver.page_source, "html.parser")

                confirmed_ipo_price = get_data("(확정)공모가격")
                subscription_rate = get_data("청약경쟁률")
                forecast_date = get_data("수요예측일")
                listing_date = get_data("상장일")
                print(f"✅ {company} 공모정보 tab 크롤링 완료")
                print(f"   ├─ (확정) 공모가격: {confirmed_ipo_price}")
                print(f"   ├─ 청약경쟁률: {subscription_rate}")
                print(f"   ├─ 수요예측일: {forecast_date}")
                print(f"   └─ 상장일: {listing_date}\n")
            except Exception as e:
                print(f"⚠ {company} 공모정보 탭 접근 오류: {e}\n")

            # 주주구성 탭으로 이동
            try:
                stockholder_info_btn = self.driver.find_element(By.XPATH, "//*[@id='print']/table/tbody/tr[5]/td/table/tbody/tr[1]/td[2]/a")
                stockholder_info_btn.click()
                time.sleep(2)
                print(f"✅ {company} 주주구성 tab 클릭 완료")
                soup = BeautifulSoup(self.driver.page_source, "html.parser")
            except Exception as e:
                print(f"⚠ {company} 주주구성 탭 접근 오류: {e}\n")

            # 공모 후 발행주식수 크롤링
            issued_shares = None
            try:
                public_after_td = soup.find("td", string="공모후")
                if public_after_td:
                    parent_tr = public_after_td.find_parent("tr")
                    next_tr = parent_tr.find_next_sibling("tr")
                    if next_tr:
                        issued_shares_td = next_tr.find("td", string="발행주식수")
                        if issued_shares_td:
                            issued_shares = issued_shares_td.find_next_sibling("td").text.strip()
                print(f"✅ {company} 주주구성 tab 크롤링 완료")
                print(f"   └─ 공모 후 발행주식수: {issued_shares}\n")
            except Exception as e:
                print(f"⚠ {company}의 공모 후 발행주식수 크롤링 오류: {e}\n")

            # JSON 데이터 저장
            company_data = {
                company: {
                    "상장일": listing_date,
                    "수요예측": {
                        "(희망)공모가격": wanted_ipo_price,
                        "단순기관경쟁률": competition_rate,
                        "의무보유확약비율": lockup_ratio
                    },
                    "공모정보": {
                        "(확정)공모가격": confirmed_ipo_price,
                        "청약경쟁률": subscription_rate,
                        "수요예측일": forecast_date,
                        "상장일": listing_date,
                        "공모후 상장주식수": issued_shares
                    }
                }
            }

            result_data.append(company_data)
            self.driver.get(self.base_url)
            time.sleep(2)

        return result_data

    
    def save_to_database(self, data):
        if not data:
            print("저장할 데이터가 없습니다.")
            return

        # NEWBIE_PROJECT 기준으로 절대 경로 설정
        base_dir = os.path.dirname(os.path.dirname(__file__))
        output_file = os.path.join(base_dir, "Finance_data", "IPOSTOCK_data.json")

        # 기존 데이터를 불러와 새로운 데이터와 병합
        if os.path.exists(output_file):
            with open(output_file, mode='r', encoding='utf-8') as file:
                existing_data = json.load(file)
            print("기존 데이터를 불러왔습니다.")
        else:
            existing_data = []

        # 기존 데이터에 새로운 데이터 추가
        combined_data = existing_data + data

        # JSON 파일에 병합된 데이터 저장
        with open(output_file, mode='w', encoding='utf-8') as file:
            json.dump(combined_data, file, ensure_ascii=False, indent=4)

        print(f"데이터가 {output_file}에 성공적으로 추가되었습니다.")

    def run(self):
        try:
            self.start_browser()

            # # 날짜 범위 설정 및 사이트 접속
            # self.select_date_range(start_date, end_date)

            # 데이터 크롤링
            data = self.scrape_data()

            # 데이터 저장
            self.save_to_database(data)
        finally:
            if self.driver:
                self.driver.quit()
                print("브라우저가 종료되었습니다.")

if __name__ == "__main__":
    output_directory = os.path.join(os.path.dirname(__file__), "output")

    os.makedirs(output_directory, exist_ok=True)

    # # 날짜 범위 설정
    # start_date = "20140204"  # 원하는 시작 날짜
    # end_date = "20160204"    # 원하는 종료 날짜

    # # 크롤링할 페이지 수를 지정
    # max_pages_to_crawl = 10  # 원하는 페이지 수로 설정

    crawler = IpostockCrawler(output_dir=output_directory)
    crawler.run()
