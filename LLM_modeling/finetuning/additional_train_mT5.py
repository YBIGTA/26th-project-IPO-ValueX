import os
os.environ['KMP_DUPLICATE_LIB_OK'] = 'True'

import torch
if torch.cuda.is_available():
    print("GPU가 사용 가능합니다. 현재 GPU:", torch.cuda.get_device_name(torch.cuda.current_device()))
else:
    print("GPU가 사용 불가능합니다. CPU를 사용합니다.")

from transformers import (
    AutoTokenizer,
    AutoModelForSeq2SeqLM,
    Seq2SeqTrainingArguments,
    Seq2SeqTrainer,
    DataCollatorForSeq2Seq
)
from peft import LoraConfig, get_peft_model
import pandas as pd
from datasets import Dataset

# 1. 기존 fine-tuned 모델 및 토크나이저 불러오기
# 토크나이저는 tokenizer 파일이 있는 폴더에서 불러옵니다.
tokenizer = AutoTokenizer.from_pretrained("./mt5_large_peft_final", use_fast=False)
# 모델은 체크포인트 폴더에서 불러옵니다.
model_checkpoint = "./mt5_large_peft_continue/checkpoint-8000"
model = AutoModelForSeq2SeqLM.from_pretrained(model_checkpoint)

if torch.cuda.is_available():
    model = model.cuda()
    print("모델이 GPU로 로드되었습니다:", torch.cuda.get_device_name(torch.cuda.current_device()))

# 2. 추가 데이터셋 로드 및 Dataset 변환
additional_data = pd.read_csv('../train_database/news_dataset_aihub_1.csv')
dataset = Dataset.from_pandas(additional_data)

# 데이터셋 분할 (예: 85% train, 15% eval)
split_dataset = dataset.train_test_split(test_size=0.15, seed=42)
train_dataset = split_dataset["train"]
eval_dataset = split_dataset["test"]
print(f"추가 데이터셋 - train: {len(train_dataset)} | eval: {len(eval_dataset)}")

# 3. 전처리 함수 정의
def preprocess_function(examples):
    inputs = examples['원문_본문']
    targets = examples['요약본_본문']
    model_inputs = tokenizer(
        inputs,
        max_length=512,
        truncation=True
    )
    with tokenizer.as_target_tokenizer():
        labels = tokenizer(
            targets,
            max_length=128,
            truncation=True
        )
    model_inputs['labels'] = labels['input_ids']
    return model_inputs

# 4. 전처리 적용
train_dataset = train_dataset.map(preprocess_function, batched=True)
eval_dataset = eval_dataset.map(preprocess_function, batched=True)

# 5. LoRA 설정
lora_config = LoraConfig(
    r=8,
    lora_alpha=32,
    lora_dropout=0.1,
    bias="none",
    task_type="SEQ_2_SEQ_LM",
    target_modules=['q', 'v']
)
model = get_peft_model(model, lora_config)

# 6. TrainingArguments 설정
# 출력 디렉토리는 체크포인트 폴더와 겹치지 않도록 별도의 폴더를 지정합니다.
training_args = Seq2SeqTrainingArguments(
    output_dir="./mt5_large_peft_continue",
    num_train_epochs=3,
    per_device_train_batch_size=2,
    per_device_eval_batch_size=2,
    eval_strategy="steps",
    eval_steps=500,
    save_steps=500,
    learning_rate=3e-5,
    bf16=True,
    logging_steps=100,
    predict_with_generate=True,
    save_total_limit=2,
    load_best_model_at_end=True,
)

data_collator = DataCollatorForSeq2Seq(tokenizer, model=model, padding=True)

# 7. Trainer 생성 및 추가 학습 진행
trainer = Seq2SeqTrainer(
    model=model,
    args=training_args,
    train_dataset=train_dataset,
    eval_dataset=eval_dataset,
    tokenizer=tokenizer,
    data_collator=data_collator,
)

# 이미 모델을 체크포인트에서 불러왔으므로, 새롭게 학습을 진행합니다.
trainer.train()

# 8. 최종 모델과 토크나이저 저장
trainer.save_model("./mt5_large_peft_final_continued")
tokenizer.save_pretrained("./mt5_large_peft_final_continued")
