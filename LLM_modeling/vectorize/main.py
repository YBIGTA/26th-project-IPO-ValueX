import os
import torch
import pandas as pd
import json
import tqdm
from multiprocessing import Pool

from article_summarize import NewsTokenizer, load_model

# 전역 변수로 모델과 토크나이저를 저장할 변수
global_tokenizer_instance = None

def init_worker(peft_model_dir):
    """각 워커에서 한 번만 모델을 로드하도록 초기화 함수"""
    global global_tokenizer_instance
    # NewsTokenizer 생성 시 모델과 토크나이저가 로드됨
    # dataset_file, output_file은 필요하지 않으므로 임의의 값 전달
    global_tokenizer_instance = NewsTokenizer(peft_model_dir, dataset_file=None, output_file=None)

def process_news_entity(news_entity):
    """단일 뉴스 엔티티를 처리하는 함수"""
    global global_tokenizer_instance
    worker_id = int(os.getpid()) % torch.cuda.device_count()
    torch.cuda.set_device(worker_id)
    print(f"Worker {os.getpid()} assigned to GPU {worker_id}")
    return global_tokenizer_instance.summarize_and_tokenize(news_entity)

def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--select_year', '-y', required=True, help='Select data year')
    parser.add_argument('--peft_model_dir', required=True, help='PEFT 모델 디렉토리 경로')
    parser.add_argument('--dataset_file', required=False, help='(필요시) 데이터셋 파일')
    parser.add_argument('--output_file', required=False, help='(필요시) 출력 파일')
    args = parser.parse_args()

    # 데이터 불러오기
    df = pd.read_csv(f'../Non_Finance_data/Naver_Stock/Processed_News/Naver_Stock_preprocessed_final_{args.select_year}.csv')

    # 뉴스 엔티티 리스트로 변환
    news_entities = df.to_dict(orient='records')

    # 병렬 처리: 워커 수는 서버 사양에 따라 조절 (예: 4)
    num_workers = 8
    with Pool(processes=num_workers, initializer=init_worker, initargs=(args.peft_model_dir,)) as pool:
        results = list(tqdm.tqdm(pool.imap(process_news_entity, news_entities), total=len(news_entities), desc="병렬 처리"))
    
    # 결과 저장
    output_directory = '../../Non_Finance_data/Naver_Stock/Summarized_News'
    os.makedirs(output_directory, exist_ok=True)
    output_file = os.path.join(output_directory, f'summarized_{args.select_year}.json')
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    
    print(f"모든 배치 처리 완료. 저장 위치: {output_file}")

if __name__ == "__main__":
    main()
