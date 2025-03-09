import os
import re
import sys
import json
import time
import math
from typing import Dict, List

from utils.logger import setup_logger

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


class ForumCrawler:
    def __init__(self, input_dir: str, output_dir: str, driver_path: str, batch_number: int = 1, batch_size: int = 5):
        """
        batch_number: 몇 번째 배치인지 (예, 1이면 첫 5개, 2이면 6~10번째 등)
        batch_size: 한 배치당 처리할 기업 수 (여기서는 5)
        """
        file_directory = os.path.join(input_dir, 'KIND_data.json')
        self.driver_path = driver_path
        self.output_dir = output_dir
        self.batch_number = batch_number
        self.batch_size = batch_size
        self.logger = setup_logger(log_file='./utils/38_Communication.log')

        if os.path.exists(file_directory):
            with open(file_directory, mode='r', encoding='utf-8') as file:
                self.file = json.load(file)
                self.logger.info('Json file loaded, total companies: %s', len(self.file))
        else:
            self.logger.error('No json file found. Exiting...')
            sys.exit(1)

    def start_browser(self):
        self.logger.info('Initialize browser...')
        chrome_options = Options()
        chrome_options.add_argument('--disable-blink-features=AutomationControlled')
        chrome_options.add_argument('--start-maximized')
        try:
            if self.driver_path and os.path.exists(self.driver_path):
                self.driver = webdriver.Chrome(
                    service=Service(self.driver_path),
                    options=chrome_options
                )
            else:
                self.driver = webdriver.Chrome(options=chrome_options)
            self.logger.info('Browser initialized successfully!')
        except Exception as e:
            self.logger.error('Something went wrong initializing the browser. Exception:')
            self.logger.error(e)
            sys.exit(1)
            
    def search_stock(self):
        """
        1. http://www.38.co.kr/ 로 이동한 후, 검색란에 주어진 종목명(예:"엘지씨엔에스")을 입력하고
        2. 검색 버튼을 클릭하여 포럼 페이지로 이동한다.
        
        기존 JSON 파일 전체가 아니라, 배치 번호에 해당하는 5개 기업만 처리합니다.
        """
        # 처리할 기업 범위 결정: 예) 배치번호 1이면 0~4, 2이면 5~9 등
        start_idx = (self.batch_number - 1) * self.batch_size
        end_idx = start_idx + self.batch_size
        batch_data = self.file[start_idx:end_idx]
        
        # 배치 내 기업명 리스트 추출
        entities = [item['기업명'] for item in batch_data]
        
        base_url = "http://www.38.co.kr/"
        self.driver.get(base_url)
        time.sleep(2)

        self.siteurl: List[Dict] = []
        Not_found_Entity: List = []

        for corp in entities:
            search_input = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.ID, "s_code"))
            )
            search_input.clear()
            search_input.send_keys(corp)
            self.logger.info(f"Keyword '{corp}'")

            search_button = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, "//input[@type='image' and @alt='검색']"))
            )

            search_button.click()
            time.sleep(3)

            try:
                # 해당 기업의 검색 결과가 있는지 체크
                self.driver.find_element(By.XPATH, '/html/body/table[3]/tbody/tr/td/table[1]/tbody/tr/td[1]/table[1]/tbody/tr[1]/td/table[2]/tbody/tr')
                found = True
            except Exception:
                found = False
                
            if not found:
                self.logger.info('Cannot find corporation name %s', corp)
                Not_found_Entity.append(corp)
            else:
                try:
                    under_board = self.driver.find_element(By.XPATH, '/html/body/table[3]/tbody/tr/td/table[1]/tbody/tr/td[1]/table[4]/tbody/tr[2]/td')
                    ref = under_board.find_elements(By.TAG_NAME, 'a')[-1].get_attribute('href')[-2:]
                    if ref[0] == "=":
                        maxpage = int(ref[-1])
                    else:
                        maxpage = int(ref)
                except Exception:
                    maxpage = 1

                self.siteurl.append({
                    '기업명': corp,
                    '사이트': self.driver.current_url,
                    '최대페이지': maxpage
                })
                
        for notfound_corp in Not_found_Entity:
            while True:
                self.logger.info('Enter the site name for %s >>> (If you want to ignore, enter [y])', notfound_corp)
                ManuallyFoundsite = input()
                if ManuallyFoundsite.lower() in ['y', 'yes']:
                    break
                elif ManuallyFoundsite.strip() == "":
                    self.logger.info("다시 입력해 주세요.")
                    continue
                else:
                    try:
                        self.driver.get(ManuallyFoundsite)
                        under_board = self.driver.find_element(By.XPATH, '/html/body/table[3]/tbody/tr/td/table[1]/tbody/tr/td[1]/table[4]/tbody/tr[2]/td')
                    except Exception:
                        self.logger.info("다시 입력해 주세요.")
                        continue
                    try:
                        ref = under_board.find_elements(By.TAG_NAME, 'a')[-1].get_attribute('href')[-2:]
                        if ref[0] == "=":
                            maxpage = int(ref[-1])
                        else:
                            maxpage = int(ref)
                    except Exception:
                        maxpage = 1
                    self.siteurl.append({
                        '기업명': notfound_corp,
                        '사이트': ManuallyFoundsite,
                        '최대페이지': maxpage
                    })
                    break

    def scrape_data(self):
        all_posts: List[Dict] = []

        for data in self.siteurl:
            Corp = data['기업명']
            Site = data['사이트']
            Maxpage = data['최대페이지']
            page_num = 1
            while True:
                current_url = f"{Site}&page={page_num}"
                self.driver.get(current_url)
                self.logger.info(f"[{Corp}] page {page_num} loaded: {current_url}")
                time.sleep(2)

                table = self.driver.find_element(By.XPATH, "/html/body/table[3]/tbody/tr/td/table[1]/tbody/tr/td[1]/table[3]")
                posts = table.find_elements(By.TAG_NAME, "tr")

                if page_num == Maxpage + 1:
                    self.logger.info(f"[{Corp}] No more page available (Page: {page_num})")
                    break
                self.logger.info(f"[{Corp}] Page {page_num}: # {len(posts)} posts found")

                for post in posts:
                    cells = post.find_elements(By.TAG_NAME, "td")
                    if len(cells) < 7:
                        continue

                    number = cells[1].text.strip()
                    title = cells[2].text.strip()
                    author = cells[3].text.strip()
                    date = cells[4].text.strip()
                    views = cells[5].text.strip()
                    recommend = cells[6].text.strip()

                    if title:
                        try:
                            link = cells[2].find_element(By.XPATH, './/a').get_attribute("href")
                            time.sleep(2)
                            self.driver.get(link)

                            body_elems = self.driver.find_elements(By.CSS_SELECTOR, 'span.readtext')
                            body = "\n".join([b.text for b in body_elems])
                        except:
                            self.logger.info(f"Failed to fetch link: {Corp} ,{title}")
                            continue
                    else:
                        continue

                    is_comment = "코멘트" in number

                    post_data = {
                        "기업명": Corp,
                        "번호": number,
                        "제목": title,
                        "글쓴이": author,
                        "날짜": date,
                        "조회": views,
                        "추천": recommend,
                        "댓글여부": is_comment,
                        "내용": body
                    }
                    all_posts.append(post_data)
                    self.driver.back()
                    time.sleep(1)

                page_num += 1

        return all_posts

    def save_to_database(self, data):
        """
        추출한 데이터를 output 폴더 내 forum_data.json 파일에 저장한다.
        기존 파일이 있으면 병합하여 저장한다.
        """
        if not data:
            print("저장할 데이터가 없습니다.")
            return

        output_file = os.path.join(self.output_dir, "forum_data.json")

        if os.path.exists(output_file):
            with open(output_file, "r", encoding="utf-8") as f:
                existing_data = json.load(f)
            print("기존 데이터를 불러왔습니다.")
        else:
            existing_data = []

        combined_data = existing_data + data
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(combined_data, f, ensure_ascii=False, indent=4)
        print(f"데이터가 {output_file}에 성공적으로 저장되었습니다.")

    def run(self):
        try:
            self.start_browser()
            self.search_stock()

            # 포럼 페이지로 이동한 후 URL에 "forum.38.co.kr"가 포함되었는지 확인
            # WebDriverWait(self.driver, 10).until(
            #     EC.url_contains("forum.38.co.kr")
            # )

            data = self.scrape_data()
            # 배치별로 데이터를 수집한 후 저장합니다.
            if data:
                self.save_to_database(data)
        finally:
            if hasattr(self, 'driver'):
                self.driver.quit()
                print("브라우저가 종료되었습니다.")



if __name__ == "__main__":
    input_directory = os.path.join('../Finance_data')
    output_directory = '../Non_Finance_data/38Comu'
    driver_executable_path = './chromedriver'
    
    batch_size = 1


    json_file = os.path.join(input_directory, 'KIND_data.json')
    if not os.path.exists(json_file):
        print("KIND_data.json 파일을 찾을 수 없습니다.")
        sys.exit(1)
        
    with open(json_file, 'r', encoding='utf-8') as f:
        companies = json.load(f)
    
    total_companies = len(companies)
    total_batches = math.ceil(total_companies / batch_size)
    print(f"총 {total_companies}개의 기업을 {batch_size}개씩 {total_batches}개의 배치로 처리합니다.")

    # 여기부터 시작!! 숫자 업데이트 수작업 ㅜㅜ
    for batch_number in range(155, total_batches + 1):
        
        print(f"\n===== 배치 {batch_number}/{total_batches} 처리 시작 =====")
        crawler = ForumCrawler(
            input_dir=input_directory, 
            output_dir=output_directory, 
            driver_path=driver_executable_path, 
            batch_number=batch_number, 
            batch_size=batch_size
        )
        crawler.run()
        print(f"===== 배치 {batch_number} 처리 완료 =====\n")