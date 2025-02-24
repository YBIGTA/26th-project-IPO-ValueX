console.log("📌 write_community.js 실행됨"); // ✅ JS 파일이 제대로 로드되는지 확인

function setCurrentDateTime() {
    console.log("📌 setCurrentDateTime() 실행됨"); // ✅ 함수 실행 확인

    const now = new Date();
    console.log("📅 new Date() 값:", now); // ✅ 현재 시간 확인

    const year = now.getFullYear();
    const month = String(now.getMonth() + 1).padStart(2, "0");
    const day = String(now.getDate()).padStart(2, "0");
    const hours = String(now.getHours()).padStart(2, "0");
    const minutes = String(now.getMinutes()).padStart(2, "0");
    const seconds = String(now.getSeconds()).padStart(2, "0");

    const formattedDateTime = `${year}-${month}-${day} ${hours}:${minutes}:${seconds}`;
    console.log("📅 최종 변환된 날짜:", formattedDateTime); // ✅ 변환된 날짜 확인

    const dateInput = document.getElementById("date");
    console.log("📌 dateInput 객체 확인:", dateInput); // ✅ 입력 필드 존재 여부 확인

    if (dateInput) {
        dateInput.value = formattedDateTime;
        console.log(`✅ 날짜 입력 완료: ${dateInput.value}`);
    } else {
        console.error("🚨 날짜 입력 필드를 찾을 수 없습니다!");
    }
}

// ✅ 상장 기업 목록 불러오기
async function loadStockList() {
    try {
        const response = await fetch("/api/ipo/list");  // ✅ 올바른 엔드포인트 사용
        const data = await response.json();

        if (data.status === "success") {
            populateStockCheckboxes(data.data);
        } else {
            console.error("🚨 종목 데이터를 불러오지 못했습니다.");
        }
    } catch (error) {
        console.error("❌ API 요청 중 오류 발생:", error);
    }
}

// ✅ 종목 목록을 버튼 스타일로 추가
function populateStockCheckboxes(stocks) {
    const stockListDiv = document.getElementById("stock-list");
    stockListDiv.innerHTML = "";  // ✅ 기존 내용 초기화

    stocks.forEach(stock => {
        const label = document.createElement("label");
        label.classList.add("stock-item");  // ✅ 스타일 적용 가능하도록 클래스 추가
        label.innerHTML = `
            <input type="radio" name="종목" value="${stock.기업명}" onclick="highlightSelected(this)"> 
            <span class="stock-btn">${stock.기업명}</span>
        `;
        stockListDiv.appendChild(label);
    });
}

// ✅ 종목 선택 시 강조 (클릭한 상태 유지)
function highlightSelected(selectedRadio) {
    const allButtons = document.querySelectorAll(".stock-btn");
    allButtons.forEach(button => button.classList.remove("selected"));  // ✅ 기존 선택 제거
    selectedRadio.nextElementSibling.classList.add("selected");  // ✅ 선택한 버튼 강조
}

// ✅ 커뮤니티 게시글 등록
document.getElementById("community-form").addEventListener("submit", async function(event) {
    event.preventDefault();

    const selectedStock = document.querySelector("input[name='종목']:checked");
    if (!selectedStock) {
        alert("📢 종목을 선택해주세요!");
        return;
    }

    const postData = {
        "종목": selectedStock.value,  // ✅ 선택된 기업명 저장
        "제목": document.getElementById("title").value,
        "내용": document.getElementById("content").value,
        "글쓴이": document.getElementById("author").value,
    };

    try {
        const response = await fetch("/api/community", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(postData),
        });

        const result = await response.json();
        if (result.status === "success") {
            alert("✅ 게시글이 등록되었습니다!");
            window.location.href = "/community";  // ✅ 커뮤니티 페이지로 이동
        } else {
            alert("🚨 오류 발생: " + result.message);
        }
    } catch (error) {
        console.error("❌ 게시글 등록 중 오류:", error);
        alert("❌ 등록 실패! 다시 시도해주세요.");
    }
});

// ✅ HTML 로드 완료 후 실행
document.addEventListener("DOMContentLoaded", function () {
    console.log("📌 DOMContentLoaded 이벤트 발생!"); // ✅ DOM이 로드되었는지 확인
    loadStockList();
    setCurrentDateTime();  // ✅ 날짜 자동 입력 실행
});