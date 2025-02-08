from selenium import webdriver
from time import sleep

class SimpleIpoCrawler:
    def __init__(self):
        self.driver = None

    def start_browser(self):
        options = webdriver.ChromeOptions()
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)
        options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.71 Safari/537.36')

        self.driver = webdriver.Chrome(options=options)
        print("브라우저가 성공적으로 시작되었습니다.")

    def open_url(self, url):
        try:
            self.driver.get(url)
            print(f"{url}에 정상적으로 접속했습니다.")
            sleep(5)  # 페이지 로딩 대기
        except Exception as e:
            print(f"URL 접속 중 오류 발생: {e}")
        finally:
            self.driver.quit()

if __name__ == "__main__":
    crawler = SimpleIpoCrawler()
    crawler.start_browser()
    # 접속할 URL 입력
    url = "http://www.ipostock.co.kr/sub03/ipo08.asp?str1=&str4=2025&str5=1"
    crawler.open_url(url)



