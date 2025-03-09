import utils as u
import torch
import json
import os
import pandas as pd
import gc
from tqdm import tqdm
from sentence_transformers import SentenceTransformer
import re

threshold = 0.15

def main():
    print('Checking existence of vectorized representitve data...')
    if os.path.exists('vectorized_repr_data.json'):
        print('Vectorized representitve data already exists. Skipping...')
    else:
        print('doing repr tokenizing')
        # u.tokenize_repr_sentence()  # SentenceTransformer 관련 로직은 main.py로 이동
        # GPU/CPU 설정 및 SentenceTransformer 로드
        device = "cuda" if torch.cuda.is_available() else "cpu"
        print(f"Using device: {device}")
        embedder = SentenceTransformer("upskyy/kf-deberta-multitask", device=device)

        repr_dict = {}
        with open('repr_sentence.txt', 'r', encoding='utf-8') as f:
            current_category = None
            for line in f:
                line = line.strip()
                if not line:
                    continue
                category_match = re.match(r"^<(.*?)>$", line)
                if category_match:
                    current_category = category_match.group(1).strip()
                    if current_category not in repr_dict:
                        repr_dict[current_category] = []
                    continue
                if current_category is None:
                    print(f"Warning: Sentence found before category definition: {line}")
                    continue
                sentences = re.split(r'\.', line)
                sentences = [s.strip() for s in sentences if s.strip()]
                for sentence in sentences:
                    embedding = embedder.encode(sentence, convert_to_tensor=True)
                    embedding = embedding.cpu().numpy().tolist()
                    repr_dict[current_category].append(embedding)

        with open('vectorized_repr_data.json', 'w', encoding='utf-8') as f:
            json.dump(repr_dict, f, ensure_ascii=False, indent=4)

    with open('vectorized_repr_data.json', 'r', encoding='utf-8') as f:
        repr_dict = json.load(f)

    # GPU 사용 가능 여부 확인 및 함수 선택
    use_gpu = torch.cuda.is_available()
    if use_gpu:
        print("Using GPU for category score calculation.")
        calculate_category_scores = u.calculate_category_scores_gpu
    else:
        print("Using CPU for category score calculation.")
        calculate_category_scores = u.calculate_category_scores_cpu


    years = sorted(os.listdir('./data'))
    for year in tqdm(years, desc="Processing years"):
        ll = u.data_list(year)
        score_list = []
        for file_path in tqdm(ll, desc=f"Processing files in {year}", leave=False):
            with open(file_path, mode='r', encoding='utf-8') as f:
                sentence_dict = json.load(f)
                Links = list(sentence_dict.keys())

                for link in Links:
                    embedding_list = sentence_dict[link]
                    # 선택된 함수 (CPU 또는 GPU) 사용
                    category_score = list(calculate_category_scores(
                        embedding_list,
                        repr_dict,
                        threshold
                    ))
                    score_list.append([link]+category_score)

            gc.collect()
        df = pd.DataFrame(score_list, columns = ['link']+ list(repr_dict.keys()))
        output_dir = './output'
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        df.to_csv(f'{output_dir}/cat_{year}.csv', encoding='utf-8', index=False)
    print("Processing completed successfully")

if __name__ == '__main__':
    main()