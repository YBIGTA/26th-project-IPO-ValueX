import json
import numpy as np
import os
import torch
from tqdm import tqdm

def data_list(year):
    file_dir = f'./data/{year}'
    files = sorted(os.listdir(file_dir))
    files = [os.path.join(file_dir, file) for file in files]
    return files

def mean_pooling(model_output, attention_mask): # SentenceTransformer 사용 시 불필요
    token_embeddings = model_output[0]
    input_mask_expanded = attention_mask.unsqueeze(-1).expand(token_embeddings.size()).float()
    return torch.sum(token_embeddings * input_mask_expanded, 1) / torch.clamp(input_mask_expanded.sum(1), min=1e-9)

def tokenize_repr_sentence():
    # SentenceTransformer 로드 및 처리는 main.py에서 수행
    pass

# --- CPU 버전 ---
def cos_sim_cpu(a, b):
    return a @ b.T / (np.linalg.norm(a, axis=1, keepdims=True) * np.linalg.norm(b, axis=1, keepdims=True).T)

def relu_cpu(arr, threshold):
    return arr * (arr >= threshold)

def calculate_category_scores_cpu(embedding_list, repr_dict, threshold: float = 0.0):
    embedding_list = np.array(embedding_list)
    N = len(embedding_list)
    num_categories = len(repr_dict)
    score_arr = np.zeros((N, num_categories))

    for i, key in enumerate(repr_dict.keys()):
        repr_arr = np.array(repr_dict[key])
        score = cos_sim_cpu(embedding_list, repr_arr)
        score_arr[:, i] = np.max(score, axis=1)

    R_score_arr = relu_cpu(score_arr, threshold)
    return np.sum(R_score_arr, axis=0) / N

# --- GPU 버전 ---
def cos_sim_gpu(a, b):
    a = torch.tensor(a, device='cuda')
    b = torch.tensor(b, device='cuda')
    return torch.matmul(a, b.T) / (torch.norm(a, dim=1, keepdim=True) * torch.norm(b, dim=1, keepdim=True).T)

def relu_gpu(arr, threshold):
    arr = torch.tensor(arr, device='cuda')
    return arr * (arr >= threshold)

def calculate_category_scores_gpu(embedding_list, repr_dict, threshold: float = 0.0):
    embedding_list = np.array(embedding_list)
    N = len(embedding_list)
    num_categories = len(repr_dict)
    score_arr = np.zeros((N, num_categories))

    for i, key in enumerate(repr_dict.keys()):
        repr_arr = np.array(repr_dict[key])
        score = cos_sim_gpu(embedding_list, repr_arr)
        score_arr[:, i] = torch.max(score, dim=1)[0].cpu().numpy()

    R_score_arr = relu_gpu(score_arr, threshold)
    return torch.sum(R_score_arr, dim=0).cpu().numpy() / N