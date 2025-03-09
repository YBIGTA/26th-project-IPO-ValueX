import torch
import pandas as pd
from sentence_transformers import SentenceTransformer
import nltk
from nltk.tokenize import sent_tokenize
import json
import os
from tqdm import tqdm

nltk.download('punkt_tab')

def download_nltk_data():
    """
    NLTK 데이터 다운로드를 위한 대화형 인터페이스를 제공합니다.
    """
    print("NLTK Data Downloader")
    print("--------------------")
    print("This script requires the 'punkt' resource from NLTK.")
    print("The interactive downloader will guide you through the process.")

    try:
        nltk.data.find('tokenizers/punkt')
        print("'punkt' resource already found.")
    except LookupError:
        nltk.download()  # 대화형 다운로더 실행


def split_into_sentences(text):
    return sent_tokenize(text)

def vectorize_news(data_dir, output_dir, start_year, end_year, model_name="upskyy/kf-deberta-multitask"):
    """
    뉴스 데이터를 읽어 문장 단위로 임베딩하고 JSON 파일로 저장합니다.
    """
    os.makedirs(output_dir, exist_ok=True)

    # GPU 사용 설정
    device = "cuda" if torch.cuda.is_available() else "cpu"
    if torch.cuda.is_available():
        print(f"Using GPU: {torch.cuda.get_device_name(torch.cuda.current_device())}")
    else:
        print("Using CPU")
    embedder = SentenceTransformer(model_name, device=device)

    for year in range(start_year, end_year + 1):
        year_output_dir = os.path.join(output_dir, str(year))
        os.makedirs(year_output_dir, exist_ok=True)

        file_path = os.path.join(data_dir, f"Naver_Stock_preprocessed_final_{year}.csv")
        try:
            df = pd.read_csv(file_path)
        except FileNotFoundError:
            print(f"파일을 찾을 수 없습니다: {file_path}")
            continue
        except Exception as e:
            print(f"파일 읽기 오류: {file_path} - {e}")
            continue

        df = df[['Link', 'Body_processed', 'tfidf_민감', 'tfidf_방어', 'tfidf_성장', 'tfidf_금융']].dropna()
        df['tfidf_sum'] = df['tfidf_민감'] + df['tfidf_방어'] + df['tfidf_성장'] + df['tfidf_금융']
        df = df[df['tfidf_sum'] > 0]

        chunk_size = 1000
        num_chunks = (len(df) + chunk_size - 1) // chunk_size

        for i in tqdm(range(num_chunks), desc=f"Processing {year}"):
            chunk = df[i * chunk_size : (i + 1) * chunk_size]
            embeddings_dict = {}

            for _, row in chunk.iterrows():
                link = row['Link']
                news_text = row['Body_processed']
                sentences = split_into_sentences(news_text)
                corpus_embeddings = embedder.encode(sentences, convert_to_tensor=True, device=device)
                embeddings_dict[link] = corpus_embeddings.cpu().numpy().tolist()

            output_file = os.path.join(year_output_dir, f"news_embeddings_{i}.json")
            with open(output_file, "w") as f:
                json.dump(embeddings_dict, f)
    print("Vectorization Complete")

if __name__ == "__main__":
    # NLTK 데이터 다운로드 (대화형)
    download_nltk_data()

    # 데이터 및 출력 경로 (Vast.ai 인스턴스 내 경로)
    #  SSH 접속 후, 실제 데이터가 있는 경로로 수정해야 합니다.
    data_dir = '../../Non_Finance_data/Naver_Stock/Processed_News'      # 예: /home/user/news_data
    output_dir = './data'  # 예: /home/user/data

    start_year = 2014
    end_year = 2025

    vectorize_news(data_dir, output_dir, start_year, end_year)