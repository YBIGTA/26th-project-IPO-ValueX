async function loadNewsList() {
    try {
        const response = await fetch("/api/news_summary");
        const data = await response.json();

        if (data.status === "success") {
            const tableBody = document.getElementById("news-table-body");
            tableBody.innerHTML = "";

            data.data.forEach((news, index) => {
                console.log(`📌 추가되는 뉴스 ${index + 1}:`, news);
                const row = document.createElement("tr");
                row.innerHTML = `
                    <td class="news-number">${index + 1}</td>
                    <td colspan="5">
                        <div class="news-container">
                            <div class="news-top">
                                <div class="news-image">
                                    <a href="${news._id}" target="_blank">
                                        <img src="${news.사진}" alt="뉴스 이미지">
                                    </a>
                                </div>
                                <div class="news-content">
                                    <div class="news-date">📅 ${news.날짜}</div>
                                    <div class="news-summary">
                                        <a href="${news.link}" target="_blank">${news.요약내용}</a>
                                    </div>
                                </div>
                            </div>
                            <div class="news-bottom">
                                <div class="news-category">
                                    📊 금융(긍정/부정): ${news.카테고리점수["금융_긍정"]?.toFixed(2) ?? "N/A"} / ${news.카테고리점수["금융_부정"]?.toFixed(2) ?? "N/A"}  
                                    | 성장(긍정/부정): ${news.카테고리점수["성장_긍정"]?.toFixed(2) ?? "N/A"} / ${news.카테고리점수["성장_부정"]?.toFixed(2) ?? "N/A"}  
                                    | 민감(긍정/부정): ${news.카테고리점수["민감_긍정"]?.toFixed(2) ?? "N/A"} / ${news.카테고리점수["민감_부정"]?.toFixed(2) ?? "N/A"}  
                                    | 방어(긍정/부정): ${news.카테고리점수["방어_긍정"]?.toFixed(2) ?? "N/A"} / ${news.카테고리점수["방어_부정"]?.toFixed(2) ?? "N/A"}
                                </div>
                                <div class="news-tags">
                                    ${(news.태그 || []).map(tag => `<span class="tag">${tag}</span>`).join("")}
                                </div>
                            </div>
                        </div>
                    </td>
                `;
                document.getElementById("news-table-body").appendChild(row);
            });
        } else {
            console.error("🚨 뉴스 데이터를 불러오지 못했습니다.");
        }
    } catch (error) {
        console.error("❌ API 요청 중 오류 발생:", error);
    }
}

// ✅ 페이지 로드 시 실행
window.onload = loadNewsList;