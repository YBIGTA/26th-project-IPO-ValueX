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

model_name = "google/mt5-large"
tokenizer = AutoTokenizer.from_pretrained(model_name, use_fast=False)
model = AutoModelForSeq2SeqLM.from_pretrained(model_name)

if torch.cuda.is_available():
    model = model.cuda()
    print(torch.cuda.is_available())

sample1 = pd.read_csv('../train_database/sample.csv')
sample1 = sample1[sample1['id'].str.contains('news_r')]
sample2 = pd.read_csv('../train_database/train_sample.csv')
dataset = Dataset.from_pandas(
    pd.concat([sample1, sample2], ignore_index=True)
)

split_dataset = dataset.train_test_split(test_size=0.15, seed=42)
train_dataset = split_dataset["train"]
eval_dataset = split_dataset["test"]
print(f"train: {len(train_dataset)} | eval: {len(eval_dataset)}")

def preprocess_function(examples):
    inputs = examples['text']
    targets = examples['summary']
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

train_dataset = train_dataset.map(preprocess_function,
                                  batched=True)
eval_dataset = eval_dataset.map(preprocess_function,
                                batched=True)


lora_config = LoraConfig(
    r=8,
    lora_alpha=32,
    lora_dropout=0.1,
    bias="none",
    task_type="SEQ_2_SEQ_LM",
    target_modules=['q', 'v']
)
model = get_peft_model(model, lora_config)

training_args = Seq2SeqTrainingArguments(
    output_dir="./mt5_large_peft",
    num_train_epochs=3,
    per_device_train_batch_size=2,
    per_device_eval_batch_size=2,
    eval_strategy="steps",
    eval_steps=500,
    save_steps=500,
    learning_rate=3e-5,
    bf16=True,
    disable_tqdm=False,
    logging_steps=100,
    predict_with_generate=True,
    save_total_limit=2,
    load_best_model_at_end=True,
)

data_collator = DataCollatorForSeq2Seq(tokenizer,
                                       model=model,
                                       padding=True)
trainer = Seq2SeqTrainer(
    model=model,
    args=training_args,
    train_dataset=train_dataset,
    eval_dataset=eval_dataset,
    tokenizer=tokenizer,
    data_collator=data_collator,
)

trainer.train()
trainer.save_model("./mt5_large_peft_final")
tokenizer.save_pretrained("./mt5_large_peft_final")