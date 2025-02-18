import numpy as np
import pandas as pd
from collections import Counter
from tqdm import tqdm

# 각 카테고리별 단어집 로드 함수
def load_category_vocab(file_paths):
    category_vocab = {}
    for category, file_path in file_paths.items():
        with open(file_path, 'r', encoding='utf-8') as f:
            category_vocab[category] = [line.strip() for line in f.readlines()]
    return category_vocab

# IDF 계산 함수
def compute_idf(docs, vocab):
    N = len(docs)
    idf = {}
    for word in vocab:
        df = sum(1 for doc in docs if word in doc)
        idf[word] = np.log((N + 1) / (df + 1)) + 1
    return idf

# Semi TF-IDF 계산 함수
def semi_tfidf(docs, category_vocab):
    total_vocab = set(word for vocab in category_vocab.values() for word in vocab)
    idf = compute_idf(docs, total_vocab)
    
    category_scores = {f'tfidf_{category}': [] for category in category_vocab.keys()}
    
    for doc in tqdm(docs, desc="📄 TF-IDF 진행"):
        words = doc.split()
        word_count = Counter(words)
        
        for category, vocab in category_vocab.items():
            score = sum(word_count[word] * idf.get(word, 0) for word in vocab)
            category_scores[f'tfidf_{category}'].append(score)
    
    return category_scores

# Semi TF-IDF 실행 함수
def run_semi_tfidf(news_file, category_files):
    df = news_file
    category_vocab = load_category_vocab(category_files)
    docs = df['Body_processed'].astype(str).tolist()
    
    category_scores = semi_tfidf(docs, category_vocab)
    
    for category, scores in category_scores.items():
        df[category] = scores  # 기존 df에 컬럼 추가
    tfidf_columns = [col for col in df.columns if col.startswith('tfidf_')]
    df = df[df[tfidf_columns].sum(axis=1) != 0]
    
    return df