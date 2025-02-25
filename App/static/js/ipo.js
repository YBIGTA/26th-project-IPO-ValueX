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
                let reg = data.data.회귀;
                let listingDate = data.data.상장일;  // ✅ 상장일 가져오기

                console.log("이진분류 원본 값:", binaryClassification);

                // 값이 존재하지 않거나 undefined/null인 경우 기본값 999 할당
                if (binaryClassification === undefined || binaryClassification === null || binaryClassification === "") {
                    console.error("이진분류 값이 비어 있음!", binaryClassification);
                    binaryClassification = 999; // 오류 발생 시 예외처리용 값
                } else {
                    binaryClassification = parseInt(binaryClassification, 10);
                }

                console.log("이진분류 변환 후 값:", binaryClassification);

                let listingInfo = `<strong style="color:black">상장일: ${listingDate}</strong><br><br>`; // ✅ 상장일 추가 (검정 글씨)

                if (binaryClassification === -1) {
                    infoBox.innerHTML = `<strong>${companyName}</strong><br><br>` + listingInfo + 
                        `⏳ <span style="color:gray">아직 결과를 알 수 없습니다.</span>`;
                } else if (binaryClassification === 0) {
                    let regText = reg >= 30 ? `<br><br>📊 예상 변동 구간: ${getRegRange(reg)}` : ""; // ✅ 회귀 값 30 이상이면 표시
                    infoBox.innerHTML = `<strong style="color:red">${companyName}</strong><br><br>` + listingInfo +
                        `❌ <span style="color:red">신중하세요! 투자 비추천</span>` + regText;
                } else if (binaryClassification === 1) {
                    let regText = `<br><br>📊 예상 변동 구간: ${getRegRange(reg)}`;
                    
                    if (reg < 0) {
                        infoBox.innerHTML = `<strong style="color:red">${companyName}</strong><br><br>` + listingInfo +
                            `❌ <span style="color:red">투자 비추천</span>` + regText;
                    } else {
                        infoBox.innerHTML = `<strong style="color:green">${companyName}</strong><br><br>` + listingInfo +
                            `✅ <span style="color:green">투자해볼 만해요! 🚀</span>` + regText;
                    }
                } else {
                    infoBox.innerHTML = `<strong style="color:orange">${companyName}</strong><br><br>` + listingInfo +
                        `⚠️ <span style="color:orange">데이터 오류: 분류 정보 없음</span>`;
                }
            } else {
                infoBox.innerHTML = "❌ 기업 정보를 찾을 수 없습니다.";
            }
        })
        .catch(error => console.error("기업 정보 불러오기 실패:", error));
}

// ✅ 회귀 값 구간 변환 함수 (30 단위 구간)
function getRegRange(value) {
    if (value < 0) return "0 이하";
    if (value < 30) return "0 ~ 30";
    if (value < 60) return "30 ~ 60";
    if (value < 90) return "60 ~ 90";
    if (value < 120) return "90 ~ 120";
    if (value < 150) return "120 ~ 150";
    if (value < 180) return "150 ~ 180";
    return "180 이상";
}