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
    def __init__(self, peft_model_dir, dataset_file, output_file, batch_size):
        self.peft_model_dir = peft_model_dir
        self.dataset_file = dataset_file
        self.output_file = output_file

        self.batch_size = batch_size

    def load_model(self):
        base_model_name = 'google/mt5-large'
        base_model = AutoModelForSeq2SeqLM.from_pretrained(base_model_name)
        self.tokenizer = AutoTokenizer.from_pretrained(base_model_name, use_fast=False)

        model = PeftModel.from_pretrained(base_model, self.peft_model_dir)
        self.device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
        self.model = model.to(self.device)

    def save(self, data):
        if not data:
            print('저장할 데이터 없음')
            return
        
        if os.path.exists(self.output_file):
            with open(self.output_file, 'w', encoding='utf-8') as f:
                existing_data = json.load(f)
        else:
            existing_data = []
        
        combined_data = existing_data + data
        with open(self.output_file, 'w', encoding='utf-8') as f:
            json.dump(combined_data, f, ensure_ascii=False, indent=4)
        print(f"데이터가 {self.output_file}에 성공적으로 저장되었습니다.")

    def get_last_state(self):
        if os.path.exists(self.output_file):
            with open(self.output_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
        else:
            print(f"{self.output_file}에 데이터 없음")
            return None
        
        if data:
            last_data = data[-1]
            return last_data
        else:
            print(f"{self.output_file}에 데이터 비어 있음")
            return None
        
    def data_preprocess(self, row):
        email_pattern = r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}"
        bracket_pattern = r"\[.*?\]"
        reporter_pattern = r"\s*[가-힣]+\s*기자\b.*$"

        title = str(row['title'])
        body = str(row['body'])

        if re.search(email_pattern, body):
            body = re.split(email_pattern, body)[0]
        body = re.sub(bracket_pattern, '', body)
        body = re.sub(reporter_pattern, '', body, flags=re.DOTALL)
        body = body.replace('nan', '')

        row['article'] = f"{title}\n{body}"

        if len(row['article'].split()) < 10:
            return None
        
        return row
    
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


    def summarize_and_tokenize(self):
        batch_size = self.batch_size
        model = self.model

        last_data = self.get_last_state()
        if last_data:
            id_ = last_data['id']
        else:
            id_ = -1
        
        article_data = pd.read_csv(
            self.dataset_file,
            encoding='utf-8'
            )
        article_data_ = article_data.iloc[id_+1: min(id_+batch_size, len(article_data))+1]
        data = []

        for _, row in tqdm(article_data_.iterrows()):
            processed_row = self.data_preprocess(row)
            if processed_row is None:
                continue

            input_text = processed_row['article']
            inputs = self.tokenizer(input_text, return_tensors="pt", truncation=True, max_length=512)
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
            data.append({
                "id": row['id'],
                "link": row['link'],
                "date": row['date'],
                "summary": summary,
                "embed_mean": mean_pool_embedding.cpu().tolist(),
                "embed_max": max_pool_embedding.cpu().tolist()
            })
        self.save(data)
        


if __name__ == '__main__':
    peft_model_dir = "../finetuning/mt5_large_peft_final"
    start, end = 2014, 2026
    dataset_files = [f"path/naverstock{year}.csv" for year in range(start, end)]
    output_files = [f"path/{file.split(".")[0]}.json" for file in dataset_files]
    batch_size = 50

    for i in range(len(dataset_files)):
        news_tokenizer = NewsTokenizer(
            peft_model_dir=peft_model_dir,
            dataset_file=dataset_files[i],
            output_file=output_files[i],
            batch_size=batch_size
        )
        news_tokenizer.load_model()

        news_tokenizer.summarize_and_tokenize()
        print(f"연도:{dataset_files[i].split(".")[-4:]}데이터 처리 완료.\n {output_files[i]}에 데이터 저장됨.")
    
    



