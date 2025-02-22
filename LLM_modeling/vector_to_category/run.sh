#!/bin/bash
# "C:\Users\Shon\Desktop"
# 데이터부터 이동
# scp -P <포트번호> -r C:\Users\Shon\Desktop\vector_analysis.zip root@<IP>:/workspace/
# apt-get update && apt-get install -y git wget
# apt install nano unzip -y
# cd /workspace

# 필요한 라이브러리 설치
echo "Installing required libraries..."
pip install pandas sentence-transformers nltk tqdm torch

export CUDA_VISIBLE_DEVICES=0

# 1. news_vectorize.py 실행 (NLTK 데이터 다운로드 및 임베딩 생성)
echo "Running news_vectorize.py..."
python news_vectorize.py

# 2. main.py 실행 (임베딩 기반 카테고리 점수 계산)
echo "Running main.py..."
python main.py

echo "All tasks completed."