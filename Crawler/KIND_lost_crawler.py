import os
import json
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium import webdriver
from time import sleep
from abc import ABC, abstractmethod

class BaseCrawler(ABC):
    def __init__(self, output_dir: str):
        self.output_dir = output_dir

    @abstractmethod
    def start_browser(self):
        pass

    @abstractmethod
    def scrape_data(self, max_pages: int):
        pass

    @abstractmethod
    def save_to_database(self, data):
        pass

class KindCrawler(BaseCrawler):
    def __init__(self, output_dir: str):
        super().__init__(output_dir)
        self.driver = None
        self.output_dir = output_dir

    def start_browser(self):
        options = webdriver.ChromeOptions()
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)
        self.driver = webdriver.Chrome(options=options)
        print("브라우저가 성공적으로 시작되었습니다.")

    def select_date_range(self, start_date: str, end_date: str):
        # 사이트 접속
        base_url = "https://kind.krx.co.kr/listinvstg/pubprcCmpStkprcByIssue.do?method=pubprcCmpStkprcByIssueMain"
        self.driver.get(base_url)
        sleep(2)  # 페이지 로딩 대기

        # 시작 날짜 입력
        start_date_input = WebDriverWait(self.driver, 10).until(
            EC.element_to_be_clickable((By.ID, "fromDate"))
        )
        ActionChains(self.driver).click(start_date_input).click(start_date_input).click(start_date_input).perform()
        sleep(1)
        start_date_input.clear()
        start_date_input.send_keys(start_date)
        print(f"시작 날짜 설정: {start_date}")

        # 종료 날짜 입력
        end_date_input = WebDriverWait(self.driver, 10).until(
            EC.element_to_be_clickable((By.ID, "toDate"))
        )
        ActionChains(self.driver).click(end_date_input).click(end_date_input).click(end_date_input).perform()
        sleep(1)
        end_date_input.clear()
        end_date_input.send_keys(end_date)
        print(f"종료 날짜 설정: {end_date}")

        # 검색 버튼 클릭
        search_button = WebDriverWait(self.driver, 10).until(
            EC.element_to_be_clickable((By.CLASS_NAME, "search-btn"))
        )
        search_button.click()
        print("검색 버튼 클릭 완료")
        sleep(15)  # 데이터 로딩 대기

    def scrape_data(self, max_pages: int):
        all_data = []  # 모든 페이지의 데이터를 저장할 리스트
        current_page = 1  # 현재 페이지 추적

        while current_page <= max_pages:
            try:
                # 테이블 로딩 대기
                WebDriverWait(self.driver, 10).until(
                    lambda d: len(d.find_elements(By.CSS_SELECTOR, "table.list tbody tr")) > 0
                )

                table = self.driver.find_element(By.CSS_SELECTOR, "table.list")
                rows = table.find_elements(By.CSS_SELECTOR, "tbody tr")

                # 현재 페이지의 데이터를 수집
                print(f"테이블에서 {len(rows)}개의 데이터 행을 발견했습니다.")

                col_names = ['기업명', '주관사', '상장일', '공모가',
                             '(수정)공모가', '(상장일)시가', '(공모가_시가)등락률',
                             '(상장일)종가', '(공모가_종가)등락률']
                for row in rows:
                    cells = row.find_elements(By.TAG_NAME, "td")
                    if len(cells) >= 9:  # 최소 9개 열을 확인
                        d = {}
                        for i in range(9):
                            d[col_names[i]] = cells[i].text.strip()
                        all_data.append(d)

                next_page_buttons = self.driver.find_elements(By.CSS_SELECTOR, "a.next")
                if next_page_buttons and next_page_buttons[0].is_enabled():
                    next_page_buttons[0].click()
                    print(f"{current_page} 페이지에서 다음 페이지로 이동합니다...")
                    current_page += 1
                    sleep(10)
                else:
                    print("다음 페이지 버튼이 비활성화되어 있거나 존재하지 않습니다. 크롤링을 종료합니다.")
                    break

            except Exception as e:
                print(f"오류 발생: {e}")
                break  # 오류 발생 시 종료

        return all_data

    def save_to_database(self, data):
        if not data:
            print("저장할 데이터가 없습니다.")
            return

         # ✅ 저장할 경로 설정
        kind_data_file = os.path.join(self.output_dir, "KIND_data.json")  # 기존 누적 데이터
        kind_lost_file = os.path.join(self.output_dir, "KIND_lost3.json")  # 새로 추가된 데이터만 저장

        # ✅ 기존 데이터 로드 (KIND_data.json)
        if os.path.exists(kind_data_file):
            with open(kind_data_file, mode='r', encoding='utf-8') as file:
                existing_data = json.load(file)
            print("기존 데이터를 불러왔습니다.")
        else:
            existing_data = []

        # ✅ 기존 데이터와 새 데이터 병합 (KIND_data.json)
        combined_data = existing_data + data

        # ✅ `KIND_data.json`에 **누적 저장**
        with open(kind_data_file, mode='w', encoding='utf-8') as file:
            json.dump(combined_data, file, ensure_ascii=False, indent=4)
        print(f"✅ 데이터가 {kind_data_file}에 누적 저장되었습니다.")

        # ✅ `KIND_lost1.json`에는 **새로 크롤링한 데이터만 저장**
        with open(kind_lost_file, mode='w', encoding='utf-8') as file:
            json.dump(data, file, ensure_ascii=False, indent=4)
        print(f"✅ 새로 크롤링한 데이터가 {kind_lost_file}에 저장되었습니다.")


    def run(self, max_pages: int, start_date: str, end_date: str):
        try:
            self.start_browser()

            # 날짜 범위 설정 및 사이트 접속 
            self.select_date_range(start_date, end_date)

            # 데이터 크롤링
            data = self.scrape_data(max_pages)

            # 데이터 저장
            self.save_to_database(data)
        finally:
            if self.driver:
                self.driver.quit()
                print("브라우저가 종료되었습니다.")

if __name__ == "__main__":
    output_directory = '../Finance_data'
    os.makedirs(output_directory, exist_ok=True)  # 폴더 생성

    # 날짜 범위 설정
    for i in range(24, 26, 2):
        start_date = "20220201"
        end_date = "20221130"

        max_pages_to_crawl = 10  # 크롤링할 페이지 수 설정

        crawler = KindCrawler(output_dir=output_directory)
        crawler.run(max_pages=max_pages_to_crawl, start_date=start_date, end_date=end_date)

