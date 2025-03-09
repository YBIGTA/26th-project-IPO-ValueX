#!/bin/bash

echo "🔹 Homebrew를 이용해 mecab 및 mecab-ko-dic 설치 중..."
brew install mecab mecab-ko mecab-ko-dic

# Mecab 사전 경로 확인
MECAB_DIC_PATH="/usr/local/lib/mecab/dic/mecab-ko-dic"

if [ -d "$MECAB_DIC_PATH" ]; then
    echo "✅ Mecab 사전이 정상적으로 설치되었습니다: $MECAB_DIC_PATH"
else
    echo "❌ Mecab 사전 설치에 실패했습니다. 수동으로 설치해야 합니다."
    exit 1
fi