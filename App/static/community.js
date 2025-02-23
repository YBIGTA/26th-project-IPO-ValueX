async function loadCommunityList() {
    try {
        const response = await fetch("/api/community");
        const data = await response.json();

        if (data.status === "success") {
            const tableBody = document.getElementById("community-table-body");
            tableBody.innerHTML = "";

            data.data.forEach((post, index) => {
                const row = document.createElement("tr");
                row.innerHTML = `
                    <td>${index + 1}</td>
                    <td>${post.종목}</td>
                    <td><a href="/community_detail?post_id=${post._id}" class="post-link">${post.제목}</a></td>
                    <td>${post.내용.substring(0, 50)}...</td>
                    <td>${post.글쓴이}</td>
                    <td>${post.날짜}</td>
                    <td>${post.조회수}</td>
                    <td>${post.추천수}</td>
                `;
                tableBody.appendChild(row);
            });
        } else {
            console.error("🚨 커뮤니티 데이터를 불러오지 못했습니다.");
        }
    } catch (error) {
        console.error("❌ API 요청 중 오류 발생:", error);
    }
}

// ✅ 페이지 로드 시 실행
window.onload = loadCommunityList;