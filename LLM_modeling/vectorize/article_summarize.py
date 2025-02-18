import torch
from transformers import AutoTokenizer, AutoModelForSeq2SeqLM
from peft import PeftModel
from typing import Dict, List

import pandas as pd
import os
import re
import json
import tqdm

class NewsTokenizer:
    def __init__(self, peft_model_dir, dataset_file, output_file):
        self.peft_model_dir = peft_model_dir
        self.dataset_file = dataset_file
        self.output_file = output_file

    def load_model(self):
        base_model_name = 'google/mt5-large'
        base_model = AutoModelForSeq2SeqLM.from_pretrained(base_model_name)
        self.tokenizer = AutoTokenizer.from_pretrained(base_model_name, use_fast=False)

        model = PeftModel.from_pretrained(base_model, self.peft_model_dir)
        self.device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
        self.model = model.to(self.device)
    
    def get_pooled_embedding(self, inputs, model):
        with torch.no_grad():
            encoder_outputs = model.encoder(
                input_ids=inputs['input_ids'],
                attention_mask=inputs['attention_mask']
            )
            last_hidden_states = encoder_outputs.last_hidden_state
        mask = inputs["attention_mask"].unsqueeze(-1).expand(last_hidden_states.size()).bool()

        # mean_pool embeddings
        sum_embeddings = torch.sum(last_hidden_states * mask, dim=1)
        sum_mask = mask.sum(dim=1).clamp(min=1e-9)
        meanpooled_vector = sum_embeddings / sum_mask
        # max_pool embeddings
        masked_hidden_states = last_hidden_states.masked_fill(~mask, -1e9)
        maxpooled_vector = masked_hidden_states.max(dim=1).values
        return meanpooled_vector, maxpooled_vector


    def summarize_and_tokenize(self, entity: dict) -> dict:
        """
        단일 뉴스 entity에 대해 전처리 수행.
        {
            'id': news id,
            'summary': news summary,
            'max_pooled_vector': [0.12, 1.3, ..., 0.03] (1024-d),
            'mean_pooled_vector': [2.1, -1.2, ..., 0.00] (1024-d)
        }
        """
        model = self.model

        article = entity['article']
        inputs = self.tokenizer(
            article,
            return_tensors='pt',
            truncation=True,
            max_length=512
        )
        inputs = {k: v.to(self.device) for k, v in inputs.items()}
        
        with torch.no_grad():
            output_ids = model.generate(
            **inputs,
            max_length=256,
            min_length=100,
            do_sample=True,
            top_k=50,
            top_p=0.95,
            no_repeat_ngram_size=3,
            early_stopping=True
        )
        summary = self.tokenizer.decode(output_ids[0], skip_special_tokens=True)
        mean_pool_embedding, max_pool_embedding = self.get_pooled_embedding(inputs, model)

        mean_pool_list = mean_pool_embedding.squeeze(0).cpu().tolist()
        max_pool_list = max_pool_embedding.squeeze(0).cpu().tolist()

        result = {
            'id': entity.get('id'),
            'summary': summary,
            'mean_pooled_vector': mean_pool_list,
            'max_pooled_vector': max_pool_list
        }
        return result