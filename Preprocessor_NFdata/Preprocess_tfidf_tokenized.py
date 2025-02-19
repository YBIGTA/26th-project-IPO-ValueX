import numpy as np
import pandas as pd
from collections import Counter
from tqdm import tqdm
from konlpy.tag import Mecab, Okt

# 각 카테고리별 단어집 로드 함수
def load_category_vocab(file_paths):
    category_vocab = {}
    for category, file_path in file_paths.items():
        with open(file_path, 'r', encoding='utf-8') as f:
            category_vocab[category] = [line.strip() for line in f.readlines()]
    return category_vocab

# 형태소 분석기 선택 (Mecab이 가장 빠름, 없으면 Okt 사용)
def get_tokenizer(tokenizer_name="mecab"):
    if tokenizer_name == "mecab":
        return Mecab(dicpath="/opt/homebrew/lib/mecab/dic/mecab-ko-dic")  # 사전 경로 지정
    elif tokenizer_name == "okt":
        return Okt()
    else:
        raise ValueError("지원하지 않는 형태소 분석기입니다. 'mecab' 또는 'okt'를 선택하세요.")

# 형태소 분석 함수
def tokenize_text(text, tokenizer, mode="noun"):
    """
    텍스트를 형태소 분석하여 원하는 품사만 추출하는 함수
    
    :param text: 분석할 텍스트
    :param tokenizer: 형태소 분석기 객체
    :param mode: 추출할 품사 ('noun' = 명사, 'morph' = 모든 형태소)
    :return: 형태소 리스트
    """
    if mode == "noun":
        return tokenizer.nouns(text)  # 명사만 추출
    elif mode == "morph":
        return tokenizer.morphs(text)  # 모든 형태소 추출
    else:
        raise ValueError("mode는 'noun' 또는 'morph' 중 하나여야 합니다.")

# IDF 계산 함수
def compute_idf(docs, vocab):
    N = len(docs)
    idf = {}
    for word in vocab:
        df = sum(1 for doc in docs if word in doc)
        idf[word] = np.log((N + 1) / (df + 1)) + 1
    return idf

# Semi TF-IDF 계산 함수
def semi_tfidf(docs, category_vocab, tokenizer, mode="noun"):
    """
    형태소 분석 기반 Semi TF-IDF 계산 함수
    
    :param docs: 문서 리스트
    :param category_vocab: 카테고리별 단어집 (dict)
    :param tokenizer: 형태소 분석기 객체
    :param mode: 형태소 분석 모드 ('noun' 또는 'morph')
    :return: 카테고리별 TF-IDF 점수
    """
    # 전체 단어 집합 생성
    total_vocab = set(word for vocab in category_vocab.values() for word in vocab)
    
    # IDF 계산
    idf = compute_idf(docs, total_vocab)
    
    # 카테고리별 점수 저장
    category_scores = {f'tfidf_{category}': [] for category in category_vocab.keys()}
    
    for doc in tqdm(docs, desc="📄 TF-IDF 진행"):
        words = tokenize_text(doc, tokenizer, mode)  # 형태소 분석 적용
        word_count = Counter(words)  # 단어 빈도 계산
        
        for category, vocab in category_vocab.items():
            score = sum(word_count[word] * idf.get(word, 0) for word in vocab)
            category_scores[f'tfidf_{category}'].append(score)
    
    return category_scores

# 실행 함수
def run_tfidf(news_file, category_files, tokenizer_name="mecab", mode="noun"):
    """
    형태소 분석 기반 Semi TF-IDF 실행 함수
    
    :param news_file: 뉴스 데이터 (DataFrame)
    :param category_files: 카테고리별 단어집 파일 경로 (dict)
    :param tokenizer_name: 사용할 형태소 분석기 ('mecab' 또는 'okt')
    :param mode: 형태소 분석 모드 ('noun' 또는 'morph')
    :return: TF-IDF 점수가 포함된 DataFrame
    """
    df = news_file
    category_vocab = load_category_vocab(category_files)
    tokenizer = get_tokenizer(tokenizer_name)
    
    # 뉴스 본문을 리스트로 변환
    docs = df['Body_processed'].astype(str).tolist()
    
    # Semi TF-IDF 계산
    category_scores = semi_tfidf(docs, category_vocab, tokenizer, mode)
    
    # 기존 DataFrame에 점수 추가
    for category, scores in category_scores.items():
        df[category] = scores
    
    # 모든 TF-IDF 점수가 0인 경우 데이터 삭제
    tfidf_columns = [col for col in df.columns if col.startswith('tfidf_')]
    df = df[df[tfidf_columns].sum(axis=1) != 0]
    
    return df