import os
import torch
import pandas as pd
import numpy as np
from sentence_transformers import SentenceTransformer
import nltk
from nltk.tokenize import sent_tokenize
import json
from tqdm import tqdm
from fastapi import HTTPException

# 현재 파일과 같은 디렉토리에 있는 JSON 파일 경로 설정
json_path = os.path.join(os.path.dirname(__file__), 'vectorized_repr_data.json')
try:
    with open(json_path, mode='r', encoding='utf-8') as f:
        repr_dict = json.load(f)
except Exception as e:
    raise HTTPException(status_code=500, detail=f"Failed to load vectorized_repr_data.json: {e}")

threshold = 0.15

def split_into_sentences(text):
    return sent_tokenize(text)

def cos_sim_cpu(a, b):
    return a @ b.T / (np.linalg.norm(a, axis=1, keepdims=True) * np.linalg.norm(b, axis=1, keepdims=True).T)

def relu_cpu(arr, threshold):
    return arr * (arr >= threshold)

def calculate_category_scores_cpu(embedding_list, repr_data, threshold_val: float = 0.0):
    embedding_array = np.array(embedding_list)
    N = len(embedding_array)
    num_categories = len(repr_data)
    score_arr = np.zeros((N, num_categories))

    for i, key in enumerate(repr_data.keys()):
        repr_arr = np.array(repr_data[key])
        score = cos_sim_cpu(embedding_array, repr_arr)
        score_arr[:, i] = np.max(score, axis=1)

    relu_scores = relu_cpu(score_arr, threshold_val)
    return np.sum(relu_scores, axis=0) / N

def single_processor(data, model_name="upskyy/kf-deberta-multitask", repr_data=None, threshold_val=None):
    """
    데이터 포인트 리스트에 대해 문장 임베딩 기반 카테고리 점수를 계산합니다.
    
    Parameters:
      - data (list): 처리할 데이터 포인트 리스트 (각 포인트는 dict 형태)
      - model_name (str): 사용할 SentenceTransformer 모델 이름
      - repr_data (dict): 벡터화된 카테고리 표현 데이터 (None이면 전역 변수 repr_dict 사용)
      - threshold_val (float): ReLU 임계값 (None이면 전역 변수 threshold 사용)
    
    Returns:
      - np.array: 평균 카테고리 점수 (유효한 데이터가 없으면 HTTPException 발생)
    """
    if repr_data is None:
        repr_data = repr_dict
    if threshold_val is None:
        threshold_val = threshold
    
    device = "cuda" if torch.cuda.is_available() else "cpu"
    try:
        embedder = SentenceTransformer(model_name, device=device)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error initializing SentenceTransformer: {e}")

    valid_count = 0
    category_scores_list = []
    for datapoint in tqdm(data, desc="Processing data points"):
        try:
            tfidf_sum = (datapoint['tfidf_민감형섹터'] + datapoint['tfidf_방어형섹터'] +
                         datapoint['tfidf_성장형섹터'] + datapoint['tfidf_금융인프라섹터'])
        except KeyError as e:
            raise HTTPException(status_code=400, detail=f"Missing key in datapoint: {e}")

        if tfidf_sum <= 0:
            continue  # TF-IDF 값이 0 이하인 경우 건너뜁니다.

        try:
            body = datapoint['Body_processed']
        except KeyError:
            raise HTTPException(status_code=400, detail="Missing 'Body_processed' in datapoint.")

        valid_count += 1
        sentences = split_into_sentences(body)
        if not sentences:
            continue
        
        try:
            corpus_embeddings = embedder.encode(sentences, convert_to_tensor=True, device=device).cpu().numpy()
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error encoding sentences: {e}")

        try:
            score = calculate_category_scores_cpu(corpus_embeddings, repr_data, threshold_val)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error calculating category scores: {e}")

        category_scores_list.append(score)
    
    if valid_count == 0 or len(category_scores_list) == 0:
        raise HTTPException(status_code=400, detail="No valid datapoints processed.")
    return (np.sum(np.array(category_scores_list), axis=0) / len(category_scores_list)).tolist()