from crawling.base_crawler import BaseCrawler
from crawling.utils.logger import setup_logger

from selenium.webdriver.chrome.webdriver import WebDriver

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys

from typing import Dict, List
import pandas as pd
import time
import os
import sys
import json

import random

from image_ocr import process_image_ocr

#-----------------------------------------
config = ('-l  kor+kor_vert+eng')

proxy_list = [
    "15.236.203.245:3128",
    "98.80.66.1:10018",
    "44.219.175.186:80"
]
#-----------------------------------------

class DcCrawler1(BaseCrawler):
    def __init__(self, output_dir: str, start_page: int = 1, end_page: int = 500, proxy: str = None):
        self.output_dir = output_dir
        self.start_page = start_page
        self.end_page = end_page  # 500페이지까지 자동 크롤링
        # 개념글, 리스트 100개씩으로 진행
        self.base_url_template = "https://gall.dcinside.com/mgallery/board/lists/?id=kospi&list_num=100&sort_type=N&exception_mode=recommend&search_head=&page={}"
        self.driver = None
        self.reviews: List[Dict[str, str]] = []
        self.logger = setup_logger(log_file='./crawling/utils/dc.log')  # Logger 설정
        self.proxy = None #random.choice(proxy_list) 

    
    def is_allowed_url(self, url: str) -> bool:
        """
        ✅ robots.txt에서 크롤링이 금지된 URL인지 확인하는 함수
        """
        disallowed_paths = [
            "/gallog/", "/api/", "/board/47", "/board/stock_new/", "/board/cat/",
            "/board/dog/", "/board/d_fighter_new1", "/board/government/", "/board/metakr/",
            "/board/salgoonews/", "/board/intl_"
        ]

        # ❌ Disallow된 경로 포함 여부 확인
        for path in disallowed_paths:
            if path in url:
                return False
        return True  # ✅ 크롤링 가능 URL

    def start_browser(self):
        """
        크롬 브라우저를 실행하는 함수
        """
        self.logger.info("브라우저를 실행합니다…")
        chrome_options = Options()
        chrome_options.add_argument('—disable-blink-features=AutomationControlled')
        # chrome_options.add_argument('—start-maximized')  # 브라우저 최대화로 시작
        chrome_options.add_argument("--headless")  # 🔹 기본적으로 크롬 창을 숨김
        chrome_options.add_argument("--disable-gpu")  # 헤드리스 모드 GPU 렌더링 방지
        chrome_options.add_argument("--window-size=1400,600")  # 크기 설정
        # chrome_options.add_argument("--incognito") #시크릿 모드 : 쿠키 없음

        # 브라우저 User-Agent 설정 (실제 브라우저 User-Agent로 변경)
        user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36"
        chrome_options.add_argument(f"user-agent={user_agent}")

        #프록시 사용
        if self.proxy:
            chrome_options.add_argument(f"--proxy-server=http://{self.proxy}")

        service = Service(ChromeDriverManager().install())  # ChromeDriver 설치 및 서비스 실행
        self.browser = webdriver.Chrome(service=service, options=chrome_options)

        # ✅ navigator.webdriver 속성 우회 (자동화 탐지 차단)
        self.browser.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

        # ✅ CDP(Chrome DevTools Protocol)에서 User-Agent 변경 적용
        self.browser.execute_cdp_cmd("Network.setUserAgentOverride", {"userAgent": user_agent})

        self.logger.info("브라우저 실행 성공!")

    def scrape_reviews(self):
        self.logger.info("크롤링 프로세스를 시작합니다…")
        self.start_browser()

        for page_num in range(self.start_page, self.end_page + 1):
            self.reviews.clear()  # 매 페이지마다 reviews 초기화
            page_url = self.base_url_template.format(page_num)
            self.logger.info(f"📌 {page_num}페이지 크롤링 시작: {page_url}")



            # Step 1: 페이지 접속
            # 🔹 페이지가 변경될 때만 브라우저를 보이게 설정
            self.browser.set_window_position(0, 0)  # 화면에 보이도록 변경
            self.browser.get(page_url)
            time.sleep(1)  # 페이지가 보일 시간을 약간 줌
            self.browser.minimize_window()  # 🔹 창을 최소화하여 숨김 
            self.logger.info(f"URL 열기 성공: {page_url}")
            time.sleep(random.uniform(2, 5))  # 랜덤 딜레이 적용 (서버 부하 방지)


        # Step 2: 목록에서 요소 받아오기 & 글 거르기(우선 나중에 처리)
            try:
                BOX_PATH = "/html/body/div[2]/div[3]/main/section[1]/article[2]/div[2]/table/tbody/tr"
                WebDriverWait(self.browser, 10).until(EC.visibility_of_element_located((By.XPATH, BOX_PATH)))
                #글 순서 파악용 지나쳐야 할 글 개수
                non_review_elements = self.browser.find_elements(By.XPATH, '//tr[@class="ub-content "]')
                pass_num = len(non_review_elements)
                self.logger.info(f"총 {pass_num}개의 글이 운영자 작성글입니다.")

                review_elements = self.browser.find_elements(By.XPATH, '//tr[@class="ub-content us-post"]')
                self.logger.info(f"총 {len(review_elements)}개의 글을 찾았습니다.")

                for i, review in enumerate(review_elements):
                    time.sleep(random.uniform(5, 10))  # ✅ Crawl-delay 준수 (랜덤 딜레이 적용)
                    try:
                        category = review.find_element(By.XPATH, f"{BOX_PATH}[{pass_num+i+1}]/td[2]").text
                        #적용 안됨 수정 필요
                        # if category.strip() in ["빨통❤", "개소리", "🍍다이"]:
                        #     pass
                        title = review.find_element(By.XPATH, f"{BOX_PATH}[{pass_num+i+1}]/td[3]/a[1]").text
                        date = review.find_element(By.XPATH, f"{BOX_PATH}[{pass_num+i+1}]/td[5]").get_attribute("title")
                        views = review.find_element(By.XPATH, f"{BOX_PATH}[{pass_num+i+1}]/td[6]").text
                        likes = review.find_element(By.XPATH, f"{BOX_PATH}[{pass_num+i+1}]/td[7]").text

                    except Exception as e:
                        self.logger.error(f"리뷰 데이터를 추출하지 못했습니다: {e}")

        # Step 3: 본문 안으로 입장
                    try:
                        page_url = review.find_element(By.XPATH, f'{BOX_PATH}[{pass_num + i + 1}]/td[3]/a[1]').get_attribute("href")

                        # ✅ `robots.txt`에서 허용된 URL만 크롤링
                        if not self.is_allowed_url(page_url):
                            self.logger.warning(f"차단된 게시판 URL: {page_url} → 크롤링 건너뜁니다.")
                            continue

                        self.browser.execute_script("window.open('');")  # 새 탭 열기
                        self.browser.switch_to.window(self.browser.window_handles[-1])
                        self.browser.get(page_url)  # 해당 URL로 이동
                        # time.sleep(1)
                        self.logger.info(f"{page_num}번째 페이지, {i+1}번째 글로 입장합니다.")


        # Step 4: 본문 안 텍스트 추출
                        try:
                            WebDriverWait(self.browser, 10).until(EC.presence_of_element_located((By.XPATH, '//div[@class="writing_view_box"]')))
                            unlikes = self.browser.find_element(By.XPATH, '//p[@class="down_num"]').text
                            # zzbang_div은 단순 그냥 사진들이라 포함 안시킴
                            contents = self.browser.find_elements(By.XPATH, '//div[@class="writing_view_box"]/div[@class="write_div"]')
                            self.logger.info(f"본문 안에서 write_div 개수: {len(contents)}개")

                            content_list = []
                            img_num = 0  # ✅ imgwrap로 시작하는 div 개수 카운트 변수
                            img_url_list = []
                            text_from_img_list = []
                            

                            for j, content in enumerate(contents):
                                # ✅ write_div 내부에 div가 있는지 확인
                                inner_divs = content.find_elements(By.XPATH, './div')
                                self.logger.info(f"본문 안에서 write_div안의 div 개수: {len(inner_divs)}개")

                                if inner_divs:  # ✅ 하위 div가 있을 경우
                                    self.logger.info("본문의 구조가 div로 구성되어있습니다.")
                                    for inner_div in inner_divs:
                                        try:
                                            # ✅ inner_div 내부의 모든 div 찾기
                                            nested_divs = inner_div.find_elements(By.XPATH, './div')
                                            # self.logger.info("지금 div안의 div의 class 이름을 확인 중입니다.")

                                            for nested_div in nested_divs:
                                                class_name = nested_div.get_attribute("class")  # ✅ div의 class 속성 가져오기

                                                # ✅ "imgwrap no"로 시작하는 div가 있는 경우 카운트 증가
                                                if class_name.startswith("imgwrap"):
                                                    img_num += 1

                                                    img_elements = nested_div.find_elements(By.XPATH, './/img')
                                                    if img_elements:
                                                        # ✅ 해당 div 내부의 img 태그 찾기
                                                        img_src = img_elements[0].get_attribute("src")

                                                        # ✅ OCR 실행
                                                        text_from_img = ""
                                                        text_from_img = process_image_ocr(img_src)

                                                        img_url_list.append(img_src)
                                                        text_from_img_list.append(text_from_img)

                                                # ✅ "ai_wrap" 클래스를 가진 div는 무시
                                                if "ai_wrap" in class_name:
                                                    continue

                                        except Exception as e:
                                            self.logger.error(f"div class 이름 확인 중 error 발생: {e}")

                                        # ✅ 하위 div의 모든 텍스트 추출
                                        text_content = inner_div.get_attribute("innerText").strip()
                                        
                                        if text_content:
                                            content_list.append(text_content)

                                else:  # ✅ 하위 div가 없을 경우, write_div 자체의 텍스트 가져오기
                                    self.logger.info("본문의 구조가 div 없이 구성되어 있습니다.")
                                    text_content = content.get_attribute("innerText").strip()
                                    if text_content:
                                        content_list.append(text_content)

                            self.logger.info(f"본문에서 텍스트를 추출했습니다: {len(content_list)}개의 문단을 저장함.")
                            self.logger.info(f"이미지 div(imgwrap) 개수: {img_num}")
                            self.logger.info(f"텍스트 추출한 이미지 수: {len(img_url_list)}")

        # # Step 5: 본문 안 이미지 추출 -> 본문 추출에서 진행하는 것으로 수정

        # Step 6: 댓글 내용 추출
                            try:
                                comments = self.browser.find_elements(By.XPATH, '//div[@class="comment_box"]//p[@class="usertxt ub-word"]')
                                comment_list = []
                                for l, comment in enumerate(comments):
                                    comment_list.append(comment.text)  # ✅ 올바른 방법
                
                                self.logger.info(f"추출된 댓글 개수: {len(comment_list)}")
                            except Exception as e:
                                self.logger.error(f"댓글 내용을 추출하지 못했습니다.: {e}")

                            self.reviews.append({
                                "현재 크롤링 페이지": page_num,
                                "말머리": category,
                                "title": title,
                                "작성시간" : date,
                                "조회수": views,
                                "추천수": likes,
                                "비추천수": unlikes,
                                "링크" : page_url,
                                "본문" : json.dumps(content_list, ensure_ascii=False),
                                "본문 내 이미지 수": img_num,
                                "이미지 링크": json.dumps(img_url_list, ensure_ascii=False),
                                "추출 텍스트 링크" : json.dumps(text_from_img_list, ensure_ascii=False),
                                "댓글" : comment_list
                            })

                        except Exception as e:
                            self.logger.error(f"본문에서 내용을 추출하지 못했습니다.: {e}")

        
        # Step 7: 페이지 닫고 복귀(다음 페이지 준비)
                        self.browser.close()
                        self.logger.info("글을 닫고 목록으로 돌아갑니다.")
                        self.logger.info("--------------------------------------------------------")
                        # time.sleep(1)
                        self.browser.switch_to.window(self.browser.window_handles[0])


                    except Exception as e:
                        self.logger.error(f"본문에 입장하지 못했습니다.: {e}")

                    # ✅ 10개 이상이면 저장 실행 (for-loop 내부에서 실행)
                    if len(self.reviews) >= 10:
                        self.logger.info("📌 10개 단위로 저장 실행")
                        self.save_to_database(page_num)

                # ✅ for-loop 종료 후, 마지막 남은 데이터 저장
                if self.reviews:  
                    self.logger.info("📌 마지막 남은 데이터 저장 실행")
                    self.save_to_database(page_num)

            except Exception as e:
                self.logger.error(f"리뷰 요소를 찾지 못했습니다: {e}")

        # Step 7: 브라우저 종료
        self.logger.info("크롤링 완료. 브라우저를 종료합니다.")
        self.browser.quit()

    def save_to_database(self, page_num):
        """
        ✅ 10개 단위로 크롤링한 리뷰 데이터를 CSV 파일로 저장
        """
        if not self.reviews:
            self.logger.info("저장할 리뷰가 없습니다.")
            return

        output_path = os.path.join(self.output_dir, f"dc_pages_{page_num}page.csv")

        # ✅ DataFrame 변환 (컬럼 순서 지정)
        df = pd.DataFrame(self.reviews, columns=[
            "현재 크롤링 페이지", "말머리", "title", "작성시간", "조회수", "추천수", "비추천수",
            "링크", "본문", "본문 내 이미지 수", "이미지 링크", "추출 텍스트 링크", "댓글"
        ])

        # ✅ CSV 파일 존재 여부 확인 후 저장 방식 결정
        file_exists = os.path.exists(output_path)

        if file_exists:
            # ✅ 기존 파일이 있으면 append (header 없이 추가)
            df.to_csv(output_path, index=False, encoding="utf-8-sig", mode="a", header=False)
            self.logger.info(f"📌 기존 CSV 파일에 10개 추가 저장: {output_path}")
        else:
            # ✅ 기존 파일이 없으면 새로운 파일 생성 (header 포함)
            df.to_csv(output_path, index=False, encoding="utf-8-sig", mode="w", header=True)
            self.logger.info(f"📌 새 CSV 파일 생성 및 저장: {output_path}")

        # ✅ 저장 후 self.reviews 초기화 (메모리 절약)
        self.reviews.clear()