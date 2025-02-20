#!/bin/bash

# 해야할 것들
# apt-get update && apt-get install -y git wget
# pip install pandas tqdm scikit-learn transformers peft sentencepiece
# cd /workspace/26th-project-IPO-ValueX/; ls 업로드 정상적인지 확인
# cd /workspace/26th-project-IPO-ValueX/LLM_modeling/vectorize/
# python3 check_gpu.py 로 gpu확인
# 

# PEFT 모델 디렉토리 경로 (사용 환경에 맞게 수정)
echo "현재 디렉토리: $(pwd)"
MODEL_DIR="../finetuning/mt5_large_peft_conti_final"

# 파이썬 스크립트 파일 이름 (예: summarize_news.py)
SCRIPT_FILE="main.py"

# 모델 디렉토리 존재 여부 확인
if [ ! -d "$MODEL_DIR" ]; then
    echo "PEFT 모델 디렉토리가 존재하지 않습니다: $MODEL_DIR"
    exit 1
fi

# 2014년부터 2025년까지 반복 실행
for year in {2014..2025}
do
    echo "처리 시작: $year 년도"
    python3 "$SCRIPT_FILE" --select_year "$year" --peft_model_dir "$MODEL_DIR"
    
    if [ $? -ne 0 ]; then
        echo "$year 년도 처리 중 에러 발생"
        exit 1
    fi

    echo "$year 년도 처리 완료"
done

echo "모든 연도에 대한 처리가 완료되었습니다."