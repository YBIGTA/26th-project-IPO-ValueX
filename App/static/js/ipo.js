document.addEventListener("DOMContentLoaded", function() {
    fetch("/api/ipo/list")
        .then(response => response.json())
        .then(data => {
            if (data.status === "success") {
                const companyList = document.getElementById("company-list");
                data.data.forEach(company => {
                    const listItem = document.createElement("li");
                    listItem.textContent = company.기업명;
                    listItem.classList.add("company-item");
                    listItem.onclick = () => fetchCompanyInfo(company.기업명);
                    companyList.appendChild(listItem);
                });
            }
        })
        .catch(error => console.error("기업 목록 불러오기 실패:", error));
});
function fetchCompanyInfo(companyName) {
    fetch(`/api/ipo/${companyName}`)
        .then(response => response.json())
        .then(data => {
            console.log("API Response:", data); // 응답 데이터 확인

            const infoBox = document.getElementById("company-info");

            if (data.status === "success") {
                let binaryClassification = data.data.이진분류;
                let regressionValue = data.data.회귀;
                
                console.log("이진분류 원본 값:", binaryClassification);
                console.log("회귀 원본 값:", regressionValue);

                // 🛑 이진분류 값이 비어 있을 경우 예외 처리
                if (binaryClassification === undefined || binaryClassification === null || binaryClassification === "") {
                    console.error("이진분류 값이 비어 있음!", binaryClassification);
                    binaryClassification = 999; // 예외처리 값
                } else {
                    binaryClassification = parseInt(binaryClassification, 10);
                }

                // 🛑 회귀 값이 비어 있을 경우 기본값 설정 & 숫자로 변환 (음수 값 포함)
                if (regressionValue === undefined || regressionValue === null || regressionValue === "") {
                    console.error("회귀 값이 비어 있음!", regressionValue);
                    regressionValue = 999; // 예외처리 값
                } else {
                    regressionValue = parseFloat(regressionValue); // ✅ `parseInt()` 대신 `parseFloat()` 사용
                    if (isNaN(regressionValue)) {
                        regressionValue = 999; // 숫자가 아니면 기본값 설정
                    }
                }

                console.log("이진분류 변환 후 값:", binaryClassification);
                console.log("회귀 변환 후 값:", regressionValue);

                // 🟢 이진분류 문구 설정
                let binaryMessage = "";
                let regressionMessage = ""; // 초기값 설정

                // 🔵 회귀 값 구간 설정
                if (regressionValue <= 0) {
                    regressionMessage = "📉 0 이하 (매우 낮음)";
                } else if (regressionValue <= 30) {
                    regressionMessage = "📉 0 ~ 30 (낮음)";
                } else if (regressionValue <= 60) {
                    regressionMessage = "📈 30 ~ 60 (보통)";
                } else if (regressionValue <= 90) {
                    regressionMessage = "📈 60 ~ 90 (약간 높음)";
                } else if (regressionValue <= 120) {
                    regressionMessage = "📈 90 ~ 120 (높음)";
                } else if (regressionValue <= 150) {
                    regressionMessage = "📈 120 ~ 150 (매우 높음)";
                } else if (regressionValue <= 180) {
                    regressionMessage = "📈 150 ~ 180 (최고 수준)";
                } else {
                    regressionMessage = "🔥 180 이상 (폭발적 성장 가능)";
                }

                if (binaryClassification === -1) {
                    binaryMessage = `<strong style="color:blue">${companyName}</strong><br><br>🕒 <span style="color:blue">아직 결과를 알 수 없습니다.</span>`;
                } else if (binaryClassification === 0) {
                    binaryMessage = `<strong style="color:red">${companyName}</strong><br><br>❌ <span style="color:red">신중하세요! 투자 비추천</span>`;
                    
                    // 🔹 회귀 값이 30 이상이면 출력
                    if (regressionValue >= 30) {
                        binaryMessage += `<br><br>📊 회귀 분석: ${regressionMessage}`;
                    }
                } else if (binaryClassification === 1) {
                    // 🛑 회귀 값이 `0 이하`라도 구간 출력
                    if (regressionValue <= 0) {
                        binaryMessage = `<strong style="color:red">${companyName}</strong><br><br>❌ <span style="color:red">신중하세요! 투자 비추천</span>`;
                    } else {
                        binaryMessage = `<strong style="color:green">${companyName}</strong><br><br>✅ <span style="color:green">투자해볼 만해요! 🚀</span>`;
                    }
                    binaryMessage += `<br><br>📊 회귀 분석: ${regressionMessage}`;
                } else {
                    binaryMessage = `<strong style="color:orange">${companyName}</strong><br><br>⚠️ <span style="color:orange">데이터 오류: 분류 정보 없음</span>`;
                }

                // 최종 출력
                infoBox.innerHTML = binaryMessage;
            } else {
                infoBox.innerHTML = "❌ 기업 정보를 찾을 수 없습니다.";
            }
        })
        .catch(error => console.error("기업 정보 불러오기 실패:", error));
}