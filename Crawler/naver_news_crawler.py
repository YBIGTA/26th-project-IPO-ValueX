from Crawler.base_crawler import BaseCrawler # 경로 절대 경로로 수정
from Crawler.utils.logger import setup_logger

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options

from typing import Dict, List

import pandas as pd
import time
import os
import sys
from tqdm import tqdm
import datetime
import random

import schedule

class NaverStockCrawler(BaseCrawler):
    def __init__(self, output_dir:str, driver_path:str=None):
        super().__init__(output_dir)
        self.base_url = "https://news.naver.com/breakingnews/section/101/258"
        self.driver = None
        self.logger = setup_logger(log_file='./Crawler/utils/naver_stock.log')
        self.driver_path = driver_path
        self.output_file = os.path.join(self.output_dir, 'Naver_Stock_2025.csv')


    def start_browser(self):
        self.logger.info('Initalize browser...')

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
                self.driver = webdriver.Chrome()
            self.logger.info('Browser initalized successfully!')
        except Exception as e:
            self.logger.info('Something gone wrong... Message below:')
            self.logger.info(e)
            sys.exit(1)

    def scrape_articles(self, date):
        self.logger.info('Starting crawling process...')
        self.start_browser()

        self.driver.get(f"{self.base_url}?date={date}")
        time.sleep(1)

        article_list = []
        self.detailed_articles = []
        news_items = self.driver.find_elements(By.CSS_SELECTOR, "div.sa_item_inner div.sa_item_flex")

        cnt = 0
        for item in tqdm(news_items, desc='Scraping Meta'):
            try:
                title_element = item.find_element(By.CSS_SELECTOR, "strong.sa_text_strong")
                title = title_element.text.strip()

                link_element = item.find_element(By.CSS_SELECTOR, "div.sa_text a")
                link = link_element.get_attribute("href")

                press_element = item.find_element(By.CSS_SELECTOR, "div.sa_text_press")
                press = press_element.text.strip() if press_element else ""

                article_list.append([title, press, link])
            except Exception:
                self.logger.info(f"[ERROR] Failed to extract article metadata, Count: {cnt}")
                cnt += 1
                pass
                

        for (title, press, link) in tqdm(article_list, desc='Scraping Details'):
            try:
                self.driver.get(link)
                time.sleep(0.7)

                try:
                    body_elem = self.driver.find_element(By.CSS_SELECTOR, "article#dic_area")
                    body = body_elem.text.strip()
                except:
                    body = ""
                    print(body, "없음")

                # try:
                #     reporter_elem = self.driver.find_element(By.XPATH, '//*[@id="dic_area"]/text()[1]')
                #     reporter = reporter_elem.text.strip()
                # except:
                #     reporter = ""

                b, d, r, ur = "", "", "", ""
                try:
                    comment_elements = self.driver.find_elements(By.CSS_SELECTOR, "div.u_cbox_comment_box.u_cbox_type_profile")
                    for item in comment_elements:
                        try:
                            b_elem = item.find_element(By.CSS_SELECTOR, "span.u_cbox_contents")
                            b += f"{b_elem.text.strip()}\n"

                            d_elem = item.find_element(By.CSS_SELECTOR, "div.u_cbox_info_base span.u_cbox_date")
                            d += f"{d_elem.get_attribute('data-value')}\n"

                            r_elem = item.find_element(By.CSS_SELECTOR, "a.u_cbox_btn_recomm em.u_cbox_cnt_recomm")
                            r += f"{r_elem.text.strip()}\n"

                            ur_elem = item.find_element(By.CSS_SELECTOR, "a.u_cbox_btn_unrecomm em.u_cbox_cnt_unrecomm")
                            ur += f"{ur_elem.text.strip()}\n"
                        except:
                            pass
                except:
                    pass

                try:
                    e_elems = self.driver.find_elements(By.CSS_SELECTOR, "span.u_likeit_list_count._count")
                    emo = [e.text.strip() for e in e_elems]
                    emotion = f"Good:{emo[5]} Warm:{emo[6]} Sad:{emo[7]} Angry:{emo[8]} Want:{emo[9]}"
                except:
                    pass
                
                try:
                    n_elems = self.driver.find_element(By.CSS_SELECTOR, "span.u_cbox_count")
                    num_comment = n_elems.text.strip()
                except:
                    num_comment = ""

                self.detailed_articles.append({
                    'Title':title,
                    'Date':date,
                    'Press':press,
                    'Link':link,
                    'Body':body,
                    'Emotion':emotion,
                    'Comment_body': b,
                    'Comment_date': d,
                    'Comment_recomm': r,
                    'Comment_unrecomm': ur,
                    'Num_comment': num_comment
                })
            except Exception as e:
                self.logger.info(f"[ERROR] Failed to extract detail article({link}): {e}")
        
        self.driver.quit()

    def save_to_database(self):
        if not self.detailed_articles:
            self.logger.info('No articles to save')

            if self.driver:
                self.driver.quit()
            return
        
        df = pd.DataFrame(self.detailed_articles)
        
        if os.path.exists(self.output_file):
            df.to_csv(self.output_file, mode='a', header=False, index=False, encoding="utf-8-sig")
        else:
            df.to_csv(self.output_file, index=False, encoding="utf-8-sig")

        self.logger.info(f"[INFO] Finished scraping | Path -> {self.output_file}")
    
    def save_to_mongodb(self):
        """
        📌 크롤링된 데이터를 MongoDB의 `raw_news_collection`에 저장 (crawler 모드에서만 실행)
        """
        if not self.detailed_articles:
            self.logger.info('No articles to save')

            if self.driver:
                self.driver.quit()
            return
        
        df = pd.DataFrame(self.detailed_articles)

        # ✅ MongoDB에 저장 (raw_news_collection)
        from Database.mongodb_connection import mongo_db  # MongoDB 연결 파일 임포트
        raw_news_collection = mongo_db.raw_news  # MongoDB 컬렉션 선택

        # MongoDB에 삽입
        raw_news_collection.insert_many(df.to_dict('records'))

        self.logger.info(f"[INFO] Finished scraping | {len(df)}개의 뉴스가 raw_news_collection에 저장됨")

    def set_start_data(self, option: str = None) -> str:
        if not option:

            if os.path.exists(self.output_file):
                df = pd.read_csv(self.output_file)
                if 'Date' in df.columns and not df.empty:
                    latest_date = df['Date'].max()
                    start_date = datetime.datetime.strptime(str(latest_date), "%Y%m%d") + datetime.timedelta(days=1)
                    return start_date.strftime("%Y%m%d")
            return "20250215"
        else:
            return option

def run_crawler(save_to_db=False):
    output_dir = "./Non_Finance_data/Naver_Stock"
    driver_path = "./chromedriver"

    crawler = NaverStockCrawler(output_dir, driver_path)
    start_date_str = crawler.set_start_data()
    start_date = datetime.datetime.strptime(start_date_str, "%Y%m%d")

    end_date = start_date + datetime.timedelta(days=1)
    today = datetime.datetime.today()

    if end_date > today:
        end_date = today

    print(f"Crawling Start: {start_date.strftime('%Y-%m-%d')} ~ {end_date.strftime('%Y-%m-%d')}")
    current_date = start_date
    while current_date < end_date:
        crawler.scrape_articles(current_date.strftime("%Y%m%d"))
        current_date += datetime.timedelta(days=1)
        print(f"Current Date: {current_date}")

    if save_to_db:
        crawler.save_to_mongodb()  # ✅ `save_to_db=True`일 때만 MongoDB 저장
        crawler.save_to_database()  # ✅ 로컬 CSV에도 저장
    else:
        crawler.save_to_database()  # 기존 방식(CSV 저장)

    print(f"✅ Finished crawling: {start_date.strftime('%Y-%m-%d')} ~ {end_date.strftime('%Y-%m-%d')}")


schedule.every(1/3600).hours.do(run_crawler)

if __name__ == "__main__":
    run_crawler()

    print("🔄")
    while True:
        schedule.run_pending()