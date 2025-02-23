async function loadCommunityPosts() {
    try {
        const response = await fetch("/api/community");
        const data = await response.json();

        const tableBody = document.getElementById("community-table-body");
        tableBody.innerHTML = ""; // 기존 내용 초기화

        data.data.forEach((post, index) => {
            const row = document.createElement("tr");

            // ✅ 게시글 클릭 시 상세 페이지로 이동
            row.addEventListener("click", () => {
                window.location.href = `/community/${post._id}`;
            });

            row.innerHTML = `
                <td>${index + 1}</td>
                <td>${post.종목}</td>
                <td>${post.제목}</td>
                <td>${post.내용}</td>
                <td>${post.글쓴이}</td>
                <td>${post.날짜}</td>
                <td>${post.조회수}</td>
                <td>${post.추천수}</td>
            `;
            tableBody.appendChild(row);
        });
    } catch (error) {
        console.error("커뮤니티 데이터를 불러오는 중 오류 발생:", error);
    }
}

// ✅ 페이지 로드 시 자동 실행
window.onload = loadCommunityPosts;