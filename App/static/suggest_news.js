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

                // ✅ 태그 처리: 문자열이면 배열로 변환, "N/A"이면 빈 배열
                let tagsArray = Array.isArray(news.태그) 
                    ? news.태그 
                    : (news.태그 && news.태그 !== "N/A" ? [news.태그] : []);

                row.innerHTML = `
                    <td class="news-number">${index + 1}</td>
                    <td colspan="5">
                        <!-- ✅ news-container를 클릭하면 뉴스 기사로 이동 -->
                        <div class="news-container" data-url="${news.link}">
                            <div class="news-top">
                                <div class="news-image">
                                    <a href="${news._id}" target="_blank">
                                        <img class = "real_news_image" src="${news.사진}" alt="뉴스 이미지">
                                    </a>
                                </div>
                                <div class="news-content">
                                    <div class="news-date">📋 ${news.날짜}</div>
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
                                    ${tagsArray.length > 0 ? tagsArray.map(tag => `<span class="tag">${tag}</span>`).join("") : "<span class='tag'>태그 없음</span>"}
                                </div>
                            </div>
                        </div>
                    </td>
                `;
                document.getElementById("news-table-body").appendChild(row);
            });

        
            // ✅ `news-container` 클릭 시 해당 뉴스 기사로 이동하도록 이벤트 추가
            document.querySelectorAll(".news-container").forEach(container => {
                container.addEventListener("click", function () {
                    const newsUrl = this.getAttribute("data-url");
                    if (newsUrl) {
                        window.open(newsUrl, "_blank"); // 새 탭에서 열기
                    }
                });
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