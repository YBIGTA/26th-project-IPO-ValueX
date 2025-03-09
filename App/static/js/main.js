// async function fetchStockData(indexName, elementId) {
//     try {
//         const response = await fetch(`/api/krx/${indexName}`);
//         const data = await response.json();

//         if (data.status === "success" && data.data) {
//             updateStockBox(elementId, data.data, indexName);
//         } else {
//             document.getElementById(elementId).textContent = `❌ 데이터 없음`;
//         }
//     } catch (error) {
//         console.error(`⚠️ ${indexName} 데이터를 불러오는 중 오류 발생:`, error);
//         document.getElementById(elementId).textContent = `⚠️ 오류 발생`;
//     }
// }

// function updateStockBox(elementId, stockData, indexName) {
//     const stockBox = document.getElementById(elementId);

//     // ✅ 데이터 부족 예외 처리
//     if (stockData.length < 2) {
//         stockBox.innerHTML = `<div style="color: gray;">❌ 데이터 부족</div>`;
//         return;
//     }
    
//     const lastPrice = stockData[stockData.length - 1]["종가"];
//     const prevPrice = stockData[stockData.length - 2]["종가"];
//     const changePercent = ((lastPrice - prevPrice) / prevPrice * 100).toFixed(2);

//     // ✅ 상승/하락 설정
//     const isUp = lastPrice > prevPrice;
//     const changeIcon = isUp ? "▲" : "▼";
//     const changeColor = isUp ? "#E1575A" : "#3D8AF7"; 

//     // ✅ 제목 옆 등락률 업데이트 (모든 지수 적용)
//     const changeDiv = document.getElementById(`${elementId}-change`);
//     if (changeDiv) {
//         changeDiv.innerHTML = `${changeIcon} ${changePercent}%`;
//         changeDiv.style.color = changeColor;
//     }

//     // ✅ 기존 내용 삭제 후 캔버스 추가
//     stockBox.innerHTML = `<canvas id="${elementId}Chart"></canvas>`;

//     drawStockChart(stockData, `${elementId}Chart`, changeColor);
// }

// // ✅ 선 색상을 반영하는 그래프 그리기 함수
// function drawStockChart(stockData, canvasId, lineColor) {
//     const labels = stockData.map(item => item["날짜"]);
//     const prices = stockData.map(item => item["종가"]);

//     const canvas = document.getElementById(canvasId);
//     const ctx = canvas.getContext("2d");

//     new Chart(ctx, {
//         type: "line",
//         data: {
//             labels: labels,
//             datasets: [{
//                 label: "", // ✅ 범례 삭제
//                 data: prices,
//                 borderColor: lineColor, // ✅ 상승(붉은색) / 하락(파란색)
//                 borderWidth: 2,
//                 fill: false,
//                 pointBackgroundColor: lineColor,  // ✅ 기본 색상 동일하게 설정 (가독성)
//                 pointBorderColor: "white",  // ✅ 테두리 흰색으로 구분
//                 pointHoverBackgroundColor: "black",  // ✅ 마우스 오버 시 검정색
//                 pointRadius: 4,  // ✅ 기본 점 크기 키움
//                 pointHoverRadius: 7,  // ✅ 마우스 오버 시 점 크기 증가
//                 hitRadius: 10  // ✅ 클릭할 수 있는 범위 확장
//             }]
//         },
//         options: {
//             responsive: true,  // ✅ 캔버스 크기 자동 조정
//             maintainAspectRatio: false,
//             plugins: {
//                 legend: { display: false }, // ✅ 범례 제거
//                 tooltip: {
//                     enabled: true,
//                     backgroundColor: "rgba(255, 255, 255, 0.9)",  // ✅ 툴팁 배경 흰색
//                     titleColor: "#000",  // ✅ 툴팁 제목 (날짜) 검정색
//                     bodyColor: "#000",  // ✅ 툴팁 본문 (종가) 검정색
//                     borderColor: "#ccc",
//                     borderWidth: 1,
//                     mode: "nearest",  // ✅ 선을 기준으로 툴팁을 표시하도록 변경
//                     intersect: false,  // ✅ 꼭 점 위가 아니어도 반응
//                     position: "average", // ✅ 마우스 커서 근처에 표시되도록 수정
//                     callbacks: {
//                         title: (tooltipItems) => tooltipItems[0].label, // ✅ 마우스 오버 시 날짜 표시
//                         label: (tooltipItem) => `${tooltipItem.raw}` // ✅ 종가 표시
//                     }
//                 }
//             },
//             scales: {
//                 x: { 
//                     display: false, // ✅ X축(날짜) 숨김
//                     grid: { drawTicks: false, drawBorder: false } // ✅ 불필요한 눈금 제거
//                 },
//                 y: { title: { display: false}} //, text: "종가 (KRW)" } }
//             },
//             hover: {
//                 mode: "nearest",  // ✅ 가까운 데이터 포인트에도 반응하도록 수정
//                 intersect: false  // ✅ 선 어디든 마우스를 올려도 반응
//             }
//         }
//     });
// }

// // ✅ 불러올 지수 목록
// const indexList = [
//     { name: "kospi", id: "kospi" },
//     { name: "kosdaq", id: "kosdaq" },
//     { name: "semiconductor", id: "1013" },
//     { name: "medical", id: "1014" },
//     { name: "chemical", id: "1008" },
//     { name: "machinery", id: "1012" },
//     { name: "service", id: "1026" },
//     { name: "food", id: "1005" },
//     { name: "banking", id: "1021" },
//     { name: "infra", id: "1045" }
// ];

// // ✅ 페이지가 로드될 때 API 호출
// window.onload = function() {
//     indexList.forEach(index => fetchStockData(index.name, index.id));
// };


async function fetchStockData(indexName, elementId, code) {
    try {
        const response = await fetch(`/api/krx/${indexName}`);
        const data = await response.json();

        if (data.status === "success" && data.data) {
            updateStockBox(elementId, data.data, code);
        } else {
            document.getElementById(elementId).textContent = `❌ 데이터 없음`;
        }
    } catch (error) {
        console.error(`⚠️ ${indexName} 데이터를 불러오는 중 오류 발생:`, error);
        document.getElementById(elementId).textContent = `⚠️ 오류 발생`;
    }
}

function updateStockBox(elementId, stockData, code) {
    const stockBox = document.getElementById(elementId);

    // ✅ 데이터 부족 예외 처리
    if (stockData.length < 2) {
        stockBox.innerHTML = `<div style="color: gray;">❌ 데이터 부족</div>`;
        return;
    }

    // ✅ 데이터에서 특정 `code` 값 찾기 (예: "KOSPI_new1001")
    const key = `KOSPI_new${code}`;
    const lastPrice = stockData[stockData.length - 1][key];
    const prevPrice = stockData[stockData.length - 2][key];

    // ✅ NaN 값 예외 처리
    if (lastPrice == null || prevPrice == null) {
        stockBox.innerHTML = `<div style="color: gray;">❌ 유효한 데이터 없음</div>`;
        return;
    }

    const changePercent = ((lastPrice - prevPrice) / prevPrice * 100).toFixed(2);

    // ✅ 상승/하락 설정
    const isUp = lastPrice > prevPrice;
    const changeIcon = isUp ? "▲" : "▼";
    const changeColor = isUp ? "#E1575A" : "#3D8AF7";

    // ✅ 제목 옆 등락률 업데이트 (모든 지수 적용)
    const changeDiv = document.getElementById(`${elementId}-change`);
    if (changeDiv) {
        changeDiv.innerHTML = `${changeIcon} ${changePercent}%`;
        changeDiv.style.color = changeColor;
    }

    // ✅ 기존 내용 삭제 후 캔버스 추가
    stockBox.innerHTML = `<canvas id="${elementId}Chart"></canvas>`;

    drawStockChart(stockData, `${elementId}Chart`, key, changeColor);
}

// ✅ 선 색상을 반영하는 그래프 그리기 함수
function drawStockChart(stockData, canvasId, key, lineColor) {
    const labels = stockData.map(item => item["date"]); // ✅ 날짜 컬럼 가져오기
    const prices = stockData.map(item => item[key]); // ✅ 해당 지수 값 가져오기

    const canvas = document.getElementById(canvasId);
    const ctx = canvas.getContext("2d");

    new Chart(ctx, {
        type: "line",
        data: {
            labels: labels,
            datasets: [{
                label: "",
                data: prices,
                borderColor: lineColor,
                borderWidth: 2,
                fill: false,
                pointBackgroundColor: lineColor,
                pointBorderColor: "white",
                pointHoverBackgroundColor: "black",
                pointRadius: 4,
                pointHoverRadius: 7,
                hitRadius: 10
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: { display: false },
                tooltip: {
                    enabled: true,
                    backgroundColor: "rgba(255, 255, 255, 0.9)",
                    titleColor: "#000",
                    bodyColor: "#000",
                    borderColor: "#ccc",
                    borderWidth: 1,
                    mode: "nearest",
                    intersect: false,
                    position: "average",
                    callbacks: {
                        title: (tooltipItems) => tooltipItems[0].label,
                        label: (tooltipItem) => `${tooltipItem.raw}`
                    }
                }
            },
            scales: {
                x: { display: false, grid: { drawTicks: false, drawBorder: false } },
                y: { title: { display: false } }
            },
            hover: { mode: "nearest", intersect: false }
        }
    });
}

// ✅ 불러올 지수 목록 (code 값 추가)
const indexList = [
    { name: "kospi", id: "kospi", code: "1001" },
    { name: "kosdaq", id: "kosdaq", code: "2001" },
    { name: "semiconductor", id: "1013", code: "1013" },
    { name: "medical", id: "1014", code: "1014" },
    { name: "chemical", id: "1008", code: "1008" },
    { name: "machinery", id: "1012", code: "1012" },
    { name: "service", id: "1026", code: "1026" },
    { name: "food", id: "1005", code: "1005" },
    { name: "banking", id: "1021", code: "1021" },
    { name: "infra", id: "1045", code: "1045" }
];

// ✅ 페이지가 로드될 때 API 호출
window.onload = function() {
    indexList.forEach(index => fetchStockData(index.name, index.id, index.code));
};