import os
import sys
import time
import datetime
import pickle
import pandas as pd
from tqdm import tqdm
from typing import Dict, List
import schedule
import pyperclip

from utils.logger import setup_logger

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import NoSuchElementException

def safe_find_text(driver, by, selector, default=""):
    try:
        return driver.find_element(by, selector).text
    except NoSuchElementException:
        return default

class NavercafeWithstockCrawler:
    def __init__(self, output_dir: str, driver_path: str = None):
        self.driver = None
        self.logger = setup_logger(log_file='./utils/navercafe_withstock.log')
        self.driver_path = driver_path
        self.output_dir = output_dir

        self.output_file = os.path.join(self.output_dir, 'Navercafe_Withstock.csv')
        
        self.detailed_articles: List[Dict] = []
        self.batch_num = 1
        self.page_number = 1

        self.max_date = '2019-12-31'

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
        
    def login(self):
        self.logger.info('Starting login process...')
        if not self.driver:
            self.start_browser()
        self.driver.get('https://nid.naver.com/nidlogin.login')
        my_ID = os.getenv('NAVER_ID', '') # ID
        my_PW = os.getenv('NAVER_PW', '') # PW
        if not my_ID or not my_PW:
            self.logger.error('NAVER_ID or NAVER_PW is not defined in environment. Exiting...')
            sys.exit(1)
        
        try:
            time.sleep(3)
            pyperclip.copy(my_ID)
            self.driver.find_element(By.CSS_SELECTOR, '#id').send_keys(Keys.CONTROL, 'v')
            time.sleep(1)
            pyperclip.copy(my_PW)
            self.driver.find_element(By.CSS_SELECTOR, '#pw').send_keys(Keys.CONTROL, 'v')
            time.sleep(1)
            self.driver.find_element(By.XPATH, '//*[@id="log.login"]').click()
            time.sleep(3)
        except Exception as e:
            self.logger.error(f'Failed to login: {e}')
    
    def save_batch(self):
        if not self.detailed_articles:
            self.logger.info(f"No articles to save")
            return
        
        df = pd.DataFrame(self.detailed_articles)
        if os.path.exists(self.output_file):
            df.to_csv(self.output_file, mode='a', header=False, index=False, encoding='utf-8-sig')
        else:
            df.to_csv(self.output_file, index=False, encoding='utf-8-sig')
        self.detailed_articles = []


    def get_last_state(self):
        try:
            df = pd.read_csv(self.output_file, encoding="utf-8-sig")
            if df.empty:
                return None, None
            last_row = df.iloc[-1]
            return last_row.get('Page', None), last_row.get('Order', None)
        except Exception as e:
            self.logger.error(f"Error reading last state from CSV: {e}")
            return None, None

    def generate_url_from_page(self, page_number: int) -> str:
        return (f"https://cafe.naver.com/withstock?"
                f"iframe_url=/ArticleList.nhn%3Fsearch.clubid=12323151%26userDisplay=50"
                f"%26search.totalCount=1001%26search.cafeld=12323151%26search.page={page_number}")

    def scrape_articles(self):
        self.logger.info('Starting crawling process...')
        max_date_str = self.max_date
        max_date = datetime.datetime.strptime(max_date_str, '%Y-%m-%d')

        self.start_browser()
        self.login()

        last_page, last_order = self.get_last_state()
        if last_page is not None:
            self.page_number = int(last_page) + 1
            self.logger.info(f"Resuming from Page {self.page_number}")
        else:
            self.page_number = 50
        
        if last_order is not None:
            self.last_order = int(last_order) + 1
            self.logger.info(f"Resuming from Order {self.last_order}")
        else:
            self.last_order = 1

        max_page = 3
        for current_page in tqdm(range(self.page_number, max_page + self.page_number), desc='Crawling...'):
            self.page_number = current_page
            current_page_url = self.generate_url_from_page(self.page_number)
            self.driver.get(current_page_url)
            time.sleep(2)

            try:
                self.driver.switch_to.frame("cafe_main")
            except Exception as e:
                self.logger.error(f"Failed to switch to frame 'cafe_main': {e}")
                continue
            self.driver.implicitly_wait(30)

            # 페이지 내 최대 50개의 글에 대해 처리
            for j in range(self.last_order, 51):
                try:
                    article_link = self.driver.find_element(
                        By.XPATH,
                        f'//*[@id="main-area"]/div[4]/table/tbody/tr[{j}]/td[1]/div[2]/div/a[1]'
                    )
                except Exception as e:
                    self.logger.info(f"No more articles found on page {self.page_number} at index {j}.")
                    break

                try:
                    # 새 탭에서 글 열기 (Ctrl + Enter)
                    article_link.send_keys(Keys.CONTROL + "\n")
                    time.sleep(1)
                    self.driver.switch_to.window(self.driver.window_handles[-1])
                    time.sleep(3.5)
                    # 새 탭에서도 cafe_main 프레임으로 전환
                    self.driver.switch_to.frame("cafe_main")
                    time.sleep(0.5)
                except:
                    break

                # 글 상세 정보 추출
                cond = True
                No_refs = ['📆출석체크📆', '가입인사(필수)', '시사금융용어', 
                           '주식초보 탈출하기', '부동산 뉴스', '부동산 투자 정보',
                           '핀업 이야기', '이용후기', '카페건의사항', '창업이야기',
                           '함투특가정보', '유진함투클럽']
                while cond:
                    # 제목 추출: 제목이 없으면 cond를 False로 만들어 루프 종료
                    link_board = safe_find_text(self.driver, By.CLASS_NAME, 'link_board')
                    if link_board in No_refs: break

                    title = safe_find_text(self.driver, By.XPATH, '/html/body/div/div/div/div[2]/div[1]/div[1]/div/div/h3', default=None)
                    if title is None:
                        cond = False
                        break
                    
                    if link_board == '뉴스/공시/지수':
                        try:
                            contents = self.driver.find_elements(By.CSS_SELECTOR, 'div.scrap_added')
                        except:
                            contents = self.driver.find_elements(By.CSS_SELECTOR, 'div.se-component.se-text.se-l-default')
                    else:
                        contents = self.driver.find_elements(By.CSS_SELECTOR, 'div.se-component.se-text.se-l-default')


                    if not contents:
                        cond = False
                        break
                    content = "\n".join([c.text for c in contents])

                    author = safe_find_text(self.driver, By.CLASS_NAME, 'nickname')
                    date = safe_find_text(self.driver, By.CLASS_NAME, 'date')
                    view = safe_find_text(self.driver, By.CLASS_NAME, 'count')
                    comment_num = safe_find_text(self.driver, By.CLASS_NAME, 'num')
                    like_count = safe_find_text(self.driver, By.CSS_SELECTOR, '.u_cnt._count')

                    if int(comment_num) > 0:
                        comments = self.driver.find_elements(By.CSS_SELECTOR, 'span.text_comment')
                        if not comments:
                            comment = ""
                        else:
                            comment = "\n".join([c.text for c in comments])
                    else:
                        comment = ""

                    article_dict = {
                        'Title': title,
                        'Content': content,
                        'Author': author,
                        'Link_board': link_board,
                        'Date': date,
                        'View': view,
                        'Comment_num': comment_num,
                        'Like_count': like_count,
                        'Comments': comment,
                        'Page': self.page_number,
                        'Order': j
                    }

                    break

                if cond == True:
                    try:
                        self.detailed_articles.append(article_dict)
                        self.logger.info("Article added: %s", len(self.detailed_articles))
                    except:
                        self.logger.info("Article content is empty, skipping.")
                else:
                    self.logger.info("Article content is empty, skipping.")

                self.driver.close()
                self.driver.switch_to.window(self.driver.window_handles[0])
                try:
                    self.driver.switch_to.frame("cafe_main")
                except Exception as e:
                    self.logger.error(f"Failed to switch back to frame 'cafe_main': {e}")

            # 일정 페이지마다 진행 상황 저장 및 브라우저 재시작
            if j == 50:
                self.detailed_articles.append({
                        'Title': "구분",
                        'Content': 'content',
                        'Author': 'author',
                        'Link_board': 'link_board',
                        'Date': 'date',
                        'View': 'view',
                        'Comment_num': 'comment_num',
                        'Like_count': 'like_count',
                        'Comments': 'comment',
                        'Page': self.page_number,
                        'Order': 0
                    })
                self.save_batch()

                try:
                    self.driver.quit()
                except Exception as e:
                    self.logger.error(f"Error quitting the browser: {e}")

                time.sleep(1)
                self.start_browser()
                self.login()
                time.sleep(1)
                # 현재 페이지 재접속
                current_page_url = self.generate_url_from_page(self.page_number)
                self.driver.get(current_page_url)
                try:
                    self.driver.switch_to.frame("cafe_main")
                except Exception as e:
                    self.logger.error(f"Failed to switch to frame after restart: {e}")

        # 크롤링 종료 후 최종 정리
        if self.driver:
            self.driver.quit()
        self.logger.info("Crawling process finished.")

    def run_scheduler(self, interval_hours: int = 0.05):
        schedule.every(interval_hours).hours.do(self.scrape_articles)
        self.logger.info(f"Scheduler started: scrape_articles will run every {interval_hours} hours - {3600 * interval_hours} seconds.")
        while True:
            schedule.run_pending()
            time.sleep(60)


if __name__ == '__main__':
    output_directory = '../Non_Finance_data'
    driver_executable_path = './chromedriver'
    
    crawler = NavercafeWithstockCrawler(output_dir=output_directory, driver_path=driver_executable_path)
    
    crawler.scrape_articles()

    crawler.run_scheduler(interval_hours=0.01)
