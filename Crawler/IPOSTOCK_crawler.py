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
from typing import List, Optional, Dict

def load_company_data(json_file):
    with open(json_file, "r", encoding="utf-8") as f:
        data = json.load(f)
        company_data = {}
        for company in data:
            company_data[company["기업명"]] = company["상장일"]
    return company_data

class IpostockCrawler(ABC):
    def __init__(self, output_dir: str):
        self.output_dir = output_dir
        self.base_url = "http://www.ipostock.co.kr/sub03/ipo08.asp?str4=2025&str5=all"  # 2025년 전체보기
        self.result: List[Dict] = []
        self.driver: Optional[webdriver.Chrome] = None
        self.company_data: Dict = {}
        self.search_fail_list: List[str] = []
        self.spac_reits: List[str] = []

    def start_browser(self,json_file:str):
        try:
            self.company_data=load_company_data(json_file)
            chrome_options = Options()
            chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.96 Safari/537.36")
            chrome_options.add_argument("accept-language=ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7")
            chrome_options.add_argument("accept-encoding=gzip, deflate, br")
            self.driver = webdriver.Chrome(options=chrome_options)
            self.driver.get(self.base_url)
            print("ipostock 페이지 로딩 완료")
        except Exception as e:
            print("ipostock 페이지 로딩 중 오류")
            raise

    def random_sleep(self, base=2, jitter=3):
        """랜덤한 대기 시간을 추가하여 요청 속도를 조절합니다."""
        sleep_time = base + random.uniform(0, jitter)
        print(f"⏳ 대기 중... {round(sleep_time, 2)}초")
        time.sleep(sleep_time)


    def search_company(self, company: str):

        search_box = self.driver.find_element(By.CLASS_NAME, "FORM1")  # 검색창 (name="str3")
        search_keyword = company
        search_box.send_keys(search_keyword)

        search_button = self.driver.find_element(By.XPATH, "//input[@type='image' and contains(@src, 'btn_search.gif')]")
        search_button.click()

        # self.random_sleep()

        # time.sleep(3) 대신: 최소한 <a> 태그가 로딩될 때까지 대기
        WebDriverWait(self.driver, 3).until(EC.presence_of_all_elements_located((By.TAG_NAME, "a")))

        soup = BeautifulSoup(self.driver.page_source, "html.parser")

        for a in soup.find_all("a"):
            found_text = a.find("font").get_text(strip=True).rstrip(".") if a.find("font") else a.get_text(strip=True).rstrip(".")
            if not found_text:
                continue
            if not company.lower().startswith(found_text.lower()):
                continue
            if company.lower().startswith(found_text.lower()):
                company_url = a.get("href")
                full_url = f"http://www.ipostock.co.kr{company_url}"
                try:
                    self.driver.get(full_url)

                    # self.random_sleep()

                    # time.sleep(2) 대신: 페이지 내에 "(희망)공모가격" 텍스트가 나타날 때까지 대기
                    WebDriverWait(self.driver, 3).until(EC.presence_of_element_located(
                        (By.XPATH, "//*[contains(text(), '(희망)공모가격')]")
                    ))
                except Exception as e:
                    print(f"⚠ {company}의 full_url 접근 중 오류 발생: {full_url}. 해당 기업은 건너뜁니다.\n")
                    self.search_fail_list.append(company)
                    return False
                print(f"\n🔍 {company} 검색 완료\n")
                return True
        print(f"⚠ {company} 검색 실패! (ipostock의 이름과 다를지도)\n")
        self.search_fail_list.append(company)
        return False

    def crawl(self, company: str):
        search_succes = self.search_company(company)

        # time.sleep(2) 대신: 페이지 내에 최소한 하나의 <td> 태그가 존재할 때까지 대기
        WebDriverWait(self.driver, 3).until(EC.presence_of_element_located((By.TAG_NAME, "td")))
        if not search_succes:
            print(f"⚠ {company} 검색에 실패했으므로 모든 데이터를 None으로 설정하여 저장\n")
            company_data = {
                company: {
                    "수요예측": {
                        "(희망)공모가격": None,
                        "단순기관경쟁률": None,
                        "의무보유확약비율": None
                    },
                    "공모정보": {
                        "(확정)공모가격": None,
                        "청약경쟁률": None,
                        "수요예측일": None,
                        "상장일": None,
                    },
                    "주주구성": {
                        "공모후 발행주식수": None,
                        "주주구성 table": None
                    },
                    "재무정보": None,
                    "종가대비등락율": None
                }
            }
            self.result.append(company_data)
            return

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

        # 공모정보 탭으로 이동
        try:
            offering_info_btn = self.driver.find_element(By.XPATH, "//*[@id='print']/table/tbody/tr[5]/td/table[1]/tbody/tr[1]/td[4]/a")
            offering_info_btn.click()
            # time.sleep(2) 대신: 페이지 내에 "(확정)공모가격" 텍스트가 나타날 때까지 대기
            WebDriverWait(self.driver, 3).until(EC.presence_of_element_located(
                (By.XPATH, "//*[contains(text(), '(확정)공모가격')]")
            ))
            print(f"✅ {company} 공모정보 tab 클릭 완료")
            soup = BeautifulSoup(self.driver.page_source, "html.parser")

            confirmed_ipo_price = get_data("(확정)공모가격")
            subscription_rate = get_data("청약경쟁률")
            forecast_date = get_data("수요예측일")
            listing_date = get_data("상장일")
            print(f"✅ {company} 공모정보 tab 크롤링 완료")
        except Exception as e:
            print(f"⚠ {company} 공모정보 탭 접근 오류\n")

        # 주주구성 탭으로 이동
        try:
            stockholder_info_btn = self.driver.find_element(By.XPATH, "//*[@id='print']/table/tbody/tr[5]/td/table/tbody/tr[1]/td[2]/a")
            stockholder_info_btn.click()
            # time.sleep(2) 대신: 페이지 내에 "공모후" 텍스트가 나타날 때까지 대기
            WebDriverWait(self.driver, 3).until(EC.presence_of_element_located(
                (By.XPATH, "//*[contains(text(), '공모후')]")
            ))
            print(f"✅ {company} 주주구성 tab 클릭 완료")
            soup = BeautifulSoup(self.driver.page_source, "html.parser")
        except Exception as e:
            print(f"⚠ {company} 주주구성 탭 접근 오류\n")

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
        except Exception as e:
            print(f"⚠ {company}의 공모 후 발행주식수 크롤링 오류\n")

        # 주주구성 테이블 크롤링
        composition_data={"보호예수매도금지":{}, "유통가능":{}}
        try:
            composition_table=soup.find_all("table", {"class": "view_tb"})[2]
            if not composition_table:
                print(f"⚠ {company} 주주구성 테이블 없음")
                return
            current_section=None 
            result1={}
            result2={}
            category=None
            for row in composition_table.find_all("tr")[2:]:
                cells=row.find_all("td")
                one_row=[]
                if cells[0].get_text(strip=True) == "보호예수매도금지":
                    category=True
                    current_section=cells[1].get_text(strip=True)
                    for cell in cells[2:]:
                        one_row.append(cell.get_text(strip=True))
                    result1[current_section]=one_row
                elif cells[0].get_text(strip=True) == "유통가능":
                    category=False
                    current_section=cells[1].get_text(strip=True)
                    for cell in cells[2:]:
                        one_row.append(cell.get_text(strip=True))
                    result2[current_section]=one_row
                elif cells[0].get_text(strip=True) in ["보호예수 물량합계","유통가능 주식합계"]:
                    continue
                else:
                    current_section=cells[0].get_text(strip=True)
                    for cell in cells[1:]:
                        one_row.append(cell.get_text(strip=True))
                    if category:
                        result1[current_section]=one_row
                    if not category:
                        result2[current_section]=one_row
            composition_data["보호예수매도금지"]=result1
            composition_data["유통가능"]=result2
        except:
            print(f"⚠ {company} 주주구성 table 크롤링 중 오류")
        
        # 재무정보 탭으로 이동
        try:
            financial_info_btn = self.driver.find_element(By.XPATH, "//*[@id='print']/table/tbody/tr[5]/td/table/tbody/tr[1]/td[3]/a")
            financial_info_btn.click()
            # time.sleep(2) 대신: 재무정보 테이블이 로딩될 때까지 대기
            WebDriverWait(self.driver, 2).until(EC.presence_of_element_located(
                (By.XPATH, "//table[@class='view_tb']")
            ))
            print(f"✅ {company} 재무정보 tab 클릭 완료")
            soup = BeautifulSoup(self.driver.page_source, "html.parser")
        except Exception as e:
            print(f"⚠ {company} 재무정보 탭 접근 오류: (아마 스팩이나 리츠)")
            self.spac_reits.append(company)

        # 재무정보 크롤링

        financial_info = {}

        def parse_value(text):
            """텍스트에서 쉼표, 불필요한 공백 제거 후 숫자로 변환 (실패하면 None)"""
            text = text.replace(",", "")
            if text in ["", "-"]:
                return None
            try:
                return int(text)
            except ValueError:
                try:
                    return float(text)
                except ValueError:
                    return None
        
        def map_label(label):
            """재무정보 테이블의 라벨을 JSON의 키에 맞게 매핑"""
            label = label.strip()
            label_clean = re.sub(r'^[\d\.\sⅠⅡⅢⅣⅤⅥⅦⅧⅨⅩ]+', '', label)
            if "유동자산" == label_clean:
                return "유동자산"
            elif "비유동자산" == label_clean:
                return "비유동자산"
            # 자산총계와 자본총계는 구분해야 함
            elif "자산총계" == label_clean:
                return "자산총계"
            elif "유동부채" == label_clean:
                return "유동부채"
            elif "비유동부채" == label_clean:
                return "비유동부채"
            elif "부채총계" == label_clean:
                return "부채총계"
            elif "자본금" == label_clean:
                return "자본금"
            elif "자본잉여금" == label_clean:
                return "자본잉여금"
            elif "이익잉여금" == label_clean:
                return "이익잉여금"
            elif "기타자본항목" == label_clean:
                return "기타자본항목"
            elif "자본총계" == label_clean:
                return "자본총계"
            elif "매출액" == label_clean:
                return "매출액"
            elif "영업이익" == label_clean:
                return "영업이익"
            elif "당기순이익" == label_clean:
                return "당기순이익"
            else:
                return None
        if soup:
            financial_table = soup.find("table", {"class": "view_tb"})
            if financial_table:
                rows = financial_table.find_all("tr")
                # 첫 2행은 헤더로 가정하여 건너뜁니다.
                data_rows = rows[2:]
                for row in data_rows:
                    tds = row.find_all("td")
                    label = tds[0].get_text(strip=True)
                    key = map_label(label)
                    if key:
                        # 각 재무정보 항목의 3개 기수(예: 제16기, 제15기, 제14기) 값을 리스트로 저장
                        values = [parse_value(td.get_text(strip=True)) for td in tds[1:4]]
                        financial_info[key] = values
                print(f"✅ {company} 재무정보 크롤링 완료\n")
            else:
                print(f"⚠ {company} 재무정보 테이블을 찾을 수 없습니다.")
        else:
            print(f"⚠ {company} 재무정보 페이지 soup 생성 실패.")

        # JSON 데이터 저장
        company_data = {
            company: {
                "수요예측": {
                    "(희망)공모가격": wanted_ipo_price,
                    "단순 기관경쟁률": competition_rate,
                    "의무보유확약비율": lockup_ratio
                },
                "공모정보": {
                    "(확정)공모가격": confirmed_ipo_price,
                    "청약경쟁률": subscription_rate,
                    "수요예측일": forecast_date,
                    "상장일": listing_date
                },
                "주주구성": {
                    "공모후 발행주식수": issued_shares,
                    "주주구성 table": composition_data
                },
                "재무정보": financial_info,
                "종가대비등락율": None
            }
        }

        self.result.append(company_data)
        self.driver.get(self.base_url)
        # time.sleep(2) 대신: 기본 페이지의 검색창(FORM1)이 로딩될 때까지 대기
        WebDriverWait(self.driver, 3).until(EC.presence_of_element_located((By.CLASS_NAME, "FORM1")))

    def scrape_data(self):
        """ 날짜에 따라 페이지 이동 후 크롤링 """
        prev_year = 2025
        for company in self.company_data.keys():
            date = datetime.strptime(self.company_data[company], "%Y-%m-%d")
            if date.year == 2025:
                self.crawl(company)
            else:
                if date.year != prev_year:
                    for _ in range(prev_year - date.year):
                        prev_btn = self.driver.find_element(By.XPATH, "//*[@id='print']/table[1]/tbody/tr[3]/td/table/tbody/tr[1]/td[1]/img[1]")
                        prev_btn.click()
                        # time.sleep(2) 대신: 기본 페이지 테이블이 로딩될 때까지 대기
                        WebDriverWait(self.driver, 3).until(EC.presence_of_element_located((By.XPATH, "//*[@id='print']/table[1]")))
                    entire_btn = self.driver.find_element(By.XPATH, "//*[@id='print']/table[1]/tbody/tr[3]/td/table/tbody/tr[1]/td[14]/a")
                    entire_btn.click()
                    # time.sleep(2) 대신: 검색창(FORM1)이 로딩될 때까지 대기
                    WebDriverWait(self.driver, 3).until(EC.presence_of_element_located((By.CLASS_NAME, "FORM1")))
                    self.base_url = self.driver.current_url
                    prev_year = date.year
                    self.crawl(company)
                else:
                    self.crawl(company)
        return self.result

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
            for json in ["KIND_data_2523.json","KIND_data_2220.json","KIND_data_1917.json","KIND_data_1614.json"]:
                self.base_url="http://www.ipostock.co.kr/sub03/ipo08.asp?str4=2025&str5=all"
                self.start_browser("./Finance_data/"+json)

                # # 날짜 범위 설정 및 사이트 접속
                # self.select_date_range(start_date, end_date)

                # 데이터 크롤링
                data = self.scrape_data()

                # 데이터 저장
                self.save_to_database(data)
                self.driver.quit()
        finally:
            print("크롤링이 종료되었습니다.\n")
            print(f"⚠ 검색 실패 목록(이름이 다르거나 없는것):{self.search_fail_list}\n")
            print(f"⚠ 재무정보 없는것(스팩 or 리츠): {self.spac_reits}")

if __name__ == "__main__":
    output_directory = os.path.join(os.path.dirname(__file__), "output")

    os.makedirs(output_directory, exist_ok=True)

    crawler = IpostockCrawler(output_dir=output_directory)
    crawler.run()

