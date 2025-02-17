import os
import json
import random
import pandas as pd
from datetime import datetime
from argparse import ArgumentParser
from typing import Dict, List
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from base_crawler import BaseCrawler
from utils.logger import setup_logger


class StockerCrawler(BaseCrawler):
    def __init__(self, output_dir: str, start_page: int = 1, end_page: int = 500, proxy: str = None):
        self.output_dir = output_dir
        self.start_page = start_page
        self.end_page = end_page
        self.base_url_template = "https://stocker.kr/stock_discuss/page/{}"
        self.driver = None
        self.reviews: List[Dict[str, str]] = []
        self.logger = setup_logger(log_file='./utils/stocker.log')
        self.proxy = proxy

    def start_browser(self):
        """✅ 크롬 브라우저 실행"""
        self.logger.info("브라우저를 실행합니다…")
        chrome_options = Options()
        chrome_options.add_argument("--headless")  # 🔹 크롬 창 숨김 (필요하면 해제)
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1400,600")
        user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36"
        chrome_options.add_argument(f"user-agent={user_agent}")

        if self.proxy:
            chrome_options.add_argument(f"--proxy-server=http://{self.proxy}")

        service = Service(ChromeDriverManager().install())
        self.browser = webdriver.Chrome(service=service, options=chrome_options)
        self.browser.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        self.browser.execute_cdp_cmd("Network.setUserAgentOverride", {"userAgent": user_agent})
        self.logger.info("브라우저 실행 성공!")

    def scrape_reviews(self):
        """✅ 크롤링 실행"""
        self.logger.info("크롤링 프로세스를 시작합니다…")
        self.start_browser()

        for page_num in range(self.start_page, self.end_page + 1):
            page_url = self.base_url_template.format(page_num)
            self.logger.info(f"📌 {page_num}페이지 크롤링 시작: {page_url}")
            self.browser.get(page_url)
            WebDriverWait(self.browser, 10).until(lambda d: d.execute_script("return document.readyState") == "complete")

            try:
                BOX_PATH = "/html/body/div[4]/main/div[1]/div[2]/div/div[2]/div/ul"
                WebDriverWait(self.browser, 10).until(EC.visibility_of_element_located((By.XPATH, BOX_PATH)))

                review_elements = self.browser.find_elements(By.XPATH, '/html/body/div[4]/main/div[1]/div[2]/div/div[2]/div/ul/li')
                self.logger.info(f"총 {len(review_elements)}개의 글을 찾았습니다.")

                for i, review in enumerate(review_elements):
                    li_class = review.get_attribute("class")
                    if li_class and "notice" in li_class:
                        self.logger.info(f"공지사항 글({i+1}번째 글) → 건너뜀")
                        continue

                    try:
                        page_url = review.find_element(By.XPATH, f'{BOX_PATH}/li[{i+1}]/a').get_attribute("href")
                        self.browser.execute_script("window.open('');")
                        self.browser.switch_to.window(self.browser.window_handles[-1])
                        self.browser.get(page_url)

                        WebDriverWait(self.browser, 10).until(EC.presence_of_element_located((By.XPATH, '//h1')))
                        self.logger.info(f"{page_num}페이지, {i+1}번째 글 크롤링 중: {page_url}")

                        title = self.browser.find_element(By.XPATH, '//h1').text
                        date = self.browser.find_element(By.XPATH, '//div[@class="app-article-meta"]/el-tooltip').get_attribute("content")
                        views = self.browser.find_element(By.XPATH, '//div[@class="app-article-meta"]/div[2]').text
                        likes = self.browser.find_element(By.XPATH, '//span[@id="vm_v_count"]').text
                        unlikes = self.browser.find_element(By.XPATH, '//span[@id="vm_d_count"]').text

                        try:
                            contents = self.browser.find_elements(By.XPATH, '//div[@class="app-article-content app-clearfix"]/div[2]/*')
                            content_list = [content.get_attribute("innerText").strip() for content in contents]
                        except:
                            content_list = []

                        try:
                            comments = self.browser.find_elements(By.XPATH, '//ul[@id="capp-board-comment-list"]/li//p')
                            comment_list = [comment.text.strip() for comment in comments if comment.text.strip()]
                        except:
                            comment_list = []

                        self.reviews.append({
                            "현재 크롤링 페이지": page_num,
                            "title": title,
                            "작성시간": date,
                            "조회수": views,
                            "추천수": likes,
                            "비추천수": unlikes,
                            "링크": page_url,
                            "본문": json.dumps(content_list, ensure_ascii=False),
                            "댓글": json.dumps(comment_list, ensure_ascii=False)
                        })

                        self.browser.close()
                        self.browser.switch_to.window(self.browser.window_handles[0])

                    except Exception as e:
                        self.logger.error(f"본문 크롤링 실패: {e}")

            except Exception as e:
                self.logger.error(f"리뷰 요소 찾기 실패: {e}")

        self.logger.info("크롤링 완료. 브라우저를 종료합니다.")
        self.browser.quit()
        self.save_to_database()

    def save_to_database(self):
        """✅ 데이터 저장 (한 파일에 누적)"""
        if not self.reviews:
            self.logger.info("저장할 리뷰가 없습니다.")
            return

        output_path = os.path.join(self.output_dir, "stocker_pages_final.csv")
        df = pd.DataFrame(self.reviews, columns=[
            "현재 크롤링 페이지", "title", "작성시간", "조회수", "추천수", "비추천수", "링크", "본문", "댓글"
        ])

        file_exists = os.path.exists(output_path)
        mode = "a" if file_exists else "w"
        header = not file_exists

        df.to_csv(output_path, index=False, encoding="utf-8-sig", mode=mode, header=header)
        self.logger.info(f"📌 크롤링 데이터 저장 완료: {output_path}")
        self.reviews.clear()


# ✅ 실행부
if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument('-o', '--output_dir', type=str, required=True, help="Output file directory.")
    parser.add_argument('-s', '--start_page', type=int, default=1, help="Start page for crawling.")
    parser.add_argument('-e', '--end_page', type=int, default=500, help="End page for crawling.")
    args = parser.parse_args()

    crawler = StockerCrawler(output_dir=args.output_dir, start_page=args.start_page, end_page=args.end_page)
    crawler.scrape_reviews()