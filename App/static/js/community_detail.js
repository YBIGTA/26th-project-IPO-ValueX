async function loadCommunityPost() {
    const urlParams = new URLSearchParams(window.location.search);
    const postId = urlParams.get("post_id");  // URL에서 post_id 가져오기

    if (!postId) {
        console.error("❌ post_id가 없습니다.");
        return;
    }

    try {
        const response = await fetch(`/api/community/${postId}`);
        const data = await response.json();

        if (data.status === "success") {
            document.getElementById("post-title").textContent = data.data.제목;
            document.getElementById("post-author").textContent = data.data.글쓴이;
            document.getElementById("post-date").textContent = data.data.날짜;
            document.getElementById("post-views").textContent = data.data.조회수;
            document.getElementById("post-likes").textContent = data.data.추천수;
            document.getElementById("post-stock").textContent = data.data.종목;
            document.getElementById("post-body").textContent = data.data.내용;
        } else {
            console.error("🚨 게시글 데이터를 불러오지 못했습니다.");
        }
    } catch (error) {
        console.error("❌ API 요청 중 오류 발생:", error);
    }
}

// ✅ 페이지 로드 시 실행
window.onload = loadCommunityPost;