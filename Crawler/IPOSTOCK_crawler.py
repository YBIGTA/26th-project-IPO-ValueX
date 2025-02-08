import os
import json
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
from time import sleep

class IpoCrawler:
    def __init__(self, output_dir, driver_path, base_url):
        self.output_dir = output_dir
        self.driver_path = driver_path
        self.driver = None
        self.base_url = base_url
        self.collected_data = []

    def start_browser(self):
        try:  # ChromeDriver 설정
            options = webdriver.ChromeOptions()
            options.add_argument('--ignore-certificate-errors')
            options.add_argument('--ignore-ssl-errors=yes')
            options.add_argument('--disable-web-security')
            options.add_argument('--allow-insecure-localhost')
            options.add_argument('--allow-running-insecure-content')
            options.add_experimental_option("excludeSwitches", ["enable-automation"])
            options.add_experimental_option('useAutomationExtension', False)
            options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.5790.170 Safari/537.36')

            
            

            self.driver = webdriver.Chrome(options=options)
            print("브라우저가 성공적으로 시작되었습니다.")
        except Exception as e:
            print(f"브라우저 시작 중 오류 발생: {e}")
            raise e

    def start(self):
        self.start_browser()  # 브라우저 시작
        self.driver.get(self.base_url)
        sleep(3)
        print(f"지정된 기업 목록 페이지에 접속했습니다.")

        # 1. Selenium으로 페이지 소스 가져와 BeautifulSoup으로 파싱
        html_source = self.driver.page_source
        soup = BeautifulSoup(html_source, 'html.parser')

        # 2. 기업명과 상세 링크 가져오기
        company_links = self.get_company_links(soup)

        # 3. 각 기업 상세 페이지에 접속하여 크롤링
        for company_name, detail_link in company_links.items():
            print(f"{company_name} 크롤링 시작")
            self.crawl_company_details(company_name, detail_link)

        # 데이터 저장
        self.save_to_json()

    def get_company_links(self, soup):
        company_links = {}
        base_url = "http://www.ipostock.co.kr"  # 상대 링크를 절대 링크로 변환하기 위한 기본 URL

        rows = soup.select("tr[height='30'][align='center']")
        for row in rows:
            try:
                link_element = row.select_one("a")
                if link_element:
                    company_name = link_element.text.strip()
                    relative_link = link_element['href']
                    
                    # 절대 URL로 변환
                    absolute_link = base_url + relative_link if not relative_link.startswith("http") else relative_link

                    company_links[company_name] = absolute_link
                    print(f"종목명: {company_name}, 링크: {absolute_link}")
            except Exception as e:
                print(f"종목 링크 추출 오류: {e}")
        return company_links

    def crawl_company_details(self, company_name, detail_link):
        try:
            self.driver.get(detail_link)
            sleep(2)

            company_data = {
                company_name: {
                    "상장일": None,
                    "수요예측": {
                        "(희망)공모가격": None,
                        "단순기관경쟁률": None,
                        "의무보유확약비율": None
                    },
                    "공모정보": {
                        "(확정)공모가격": None,
                        "청약경쟁률": None,
                        "수요예측일": None,
                        "상장일": None
                    },
                    "주주구성": {
                        "공모후 발행주식수": None
                    },
                    "재무정보": {}
                }
            }

            # 각 탭별 데이터 수집
            self.click_tab("수요예측")
            company_data[company_name]["수요예측"] = self.scrape_demand_forecast()

            self.click_tab("공모정보")
            company_data[company_name]["공모정보"] = self.scrape_public_offering_info()

            self.click_tab("주주구성")
            company_data[company_name]["주주구성"] = self.scrape_shareholder_info()

            self.click_tab("재무정보")
            company_data[company_name]["재무정보"] = self.scrape_financial_info()

            self.collected_data.append(company_data)

        except Exception as e:
            print(f"{company_name} 세부 정보 크롤링 중 오류 발생: {e}")

    def click_tab(self, tab_name):
        tab_mapping = {
            "수요예측": 'view_05.asp',
            "공모정보": 'view_04.asp',
            "주주구성": 'view_02.asp',
            "재무정보": 'view_03.asp'
        }
        tab_url = tab_mapping.get(tab_name)
        if tab_url:
            current_url = self.driver.current_url
            code = current_url.split("code=")[-1].split("&")[0]
            tab_link = f"http://www.ipostock.co.kr/sub03/{tab_url}?code={code}"
            self.driver.get(tab_link)
            sleep(2)

    def scrape_demand_forecast(self):
        try:
            return {
                "(희망)공모가격": self.extract_text('//tr[td/font[contains(text(), "(희망)공모가격")]]/td[2]'),
                "단순기관경쟁률": self.extract_text('//tr[td/font[contains(text(), "단순 기관경쟁률")]]/td[2]'),
                "의무보유확약비율": self.extract_text('//tr[td/font[contains(text(), "의무보유확약비율")]]/td[2]')
            }
        except Exception as e:
            print(f"수요예측 정보 수집 실패: {e}")
            return {}

    def scrape_public_offering_info(self):
        try:
            return {
                "(확정)공모가격": self.extract_text('//tr[td/font[contains(text(), "(확정)공모가격")]]/td[2]'),
                "청약경쟁률": self.extract_text('//tr[td/font[contains(text(), "청약경쟁률")]]/td[2]'),
                "수요예측일": self.extract_text('//tr[td/font[contains(text(), "수요예측일")]]/td[2]'),
                "상장일": self.extract_text('//tr[td/font[contains(text(), "상장일")]]/td[2]')
            }
        except Exception as e:
            print(f"공모정보 수집 실패: {e}")
            return {}

    def scrape_shareholder_info(self):
        try:
            shares_after_ipo = self.extract_text('//tr[td[contains(text(), "공모후")]]/td[3]')
            return {"공모후 발행주식수": shares_after_ipo}
        except Exception as e:
            print(f"주주구성 정보 수집 실패: {e}")
            return {}

    def scrape_financial_info(self):
        try:
            table = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "table.view_tb"))
            )
            rows = table.find_elements(By.TAG_NAME, "tr")
            data_dict = {}
            for row in rows[2:]:
                cells = row.find_elements(By.TAG_NAME, "td")
                if len(cells) == 4:
                    item_name = cells[0].text.strip()
                    values = [cells[i].text.strip() for i in range(1, 4)]
                    data_dict[item_name] = values
            return data_dict
        except Exception as e:
            print(f"재무정보 수집 실패: {e}")
            return {}

    def extract_text(self, xpath):
        try:
            element = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.XPATH, xpath))
            )
            return element.text.strip()
        except Exception:
            return None

    def save_to_json(self):
        base_dir = os.path.dirname(os.path.dirname(__file__))
        output_file = os.path.join(base_dir, "Finance_data", "IPOSTOCK_data.json")
        os.makedirs(os.path.dirname(output_file), exist_ok=True)

        with open(output_file, mode='w', encoding='utf-8') as file:
            json.dump(self.collected_data, file, ensure_ascii=False, indent=4)
        print(f"데이터가 {output_file}에 저장되었습니다.")

if __name__ == "__main__":
    driver_path = r"C:\WebDriver\chromedriver.exe"  # 크롬드라이버 경로 설정
    output_dir = os.path.join(os.getcwd(), "output")

    # 직접 링크 입력
    base_url = input("크롤링할 URL을 입력하세요 (예: https://www.ipostock.co.kr/sub03/ipo08.asp?str1=&str4=2025&str5=2): ")

    ipo_crawler = IpoCrawler(output_dir, driver_path, base_url=base_url)
    ipo_crawler.start()






