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
            const infoBox = document.getElementById("company-info");
            if (data.status === "success") {
                infoBox.innerHTML = `<strong>${companyName}</strong><br>상장일: ${data.data.상장일}`;
            } else {
                infoBox.innerHTML = "❌ 기업 정보를 찾을 수 없습니다.";
            }
        })
        .catch(error => console.error("기업 정보 불러오기 실패:", error));
}