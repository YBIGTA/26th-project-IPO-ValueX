from selenium import webdriver
from time import sleep

def main():
    options = webdriver.ChromeOptions()
    options.add_argument('--ignore-certificate-errors')  # SSL 오류 무시
    options.add_argument('--ignore-ssl-errors=yes')
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.71 Safari/537.36')

    # WebDriver 실행 (경로 설정 필요 없음)
    driver = webdriver.Chrome(options=options)
    print("브라우저가 성공적으로 시작되었습니다.")

    # 테스트할 페이지로 이동
    driver.get("http://www.ipostock.co.kr/sub03/ipo08.asp?str1=&str4=2025&str5=1")
    sleep(3)  # 페이지 로드 대기

    print("지정된 URL에 접속했습니다.")

    # 종료
    driver.quit()
    print("브라우저가 종료되었습니다.")

if __name__ == "__main__":
    main()
