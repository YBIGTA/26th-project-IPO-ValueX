import torch
from transformers import AutoTokenizer, AutoModelForSeq2SeqLM
from peft import PeftModel

peft_model_dir = './mt5_large_peft'

base_model_name = "google/mt5-large"
base_model = AutoModelForSeq2SeqLM.from_pretrained(base_model_name)
tokenizer = AutoTokenizer.from_pretrained(base_model_name, use_fast=False)

model = PeftModel.from_pretrained(base_model, peft_model_dir)

device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
model = model.to(device)

input_text = """

기관투자가 자격으로 공모주 청약에 참여해 수익을 올리는 공모주펀드 설정액이 최근 6개월간 5000억 원 넘게 감소한 것으로 확인됐다. 지난해 하반기부터 기업공개(IPO) 시장에 한파가 이어지며 투자자 관심이 줄고 있는 데다 대어로 주목받았던 LG CNS(LG씨엔에스(064400))가 상장 첫날부터 부진한 모습을 보이자 투심이 더욱 위축되는 모습이다. 시장 전문가들은 지난해 상반기 같은 IPO 시장 호황을 당분간 기대하기는 어렵다며 선별 투자에 임할 것을 당부했다.
14일 금융정보 업체 에프앤가이드에 따르면 이날 기준 공모주펀드 155개의 설정액은 3조 7923억 원이다. 6개월 전인 지난해 8월 17일 대비 5296억 원 감소한 수치다. 공모주펀드는 올 들어서도 2개월여간 820억 원이 빠져나가며 부진한 모습이다.
이는 지난해에 이어 올해도 IPO 시장 반등 기미가 보이지 않자 투자자들이 계속해서 자금을 빼기 때문이다. 한국거래소에 따르면 올 들어 기업인수목적회사(SPAC·스팩)를 제외하고 코스피와 코스닥 시장에 새로 입성한 종목은 총 11개로 이 중 70%가 넘는 8개 종목이 상장 첫날 공모가보다 낮은 가격에 거래를 마쳤다. 온라인 교육 콘텐츠 업체 데이원컴퍼니(373160)의 경우 상장 첫날 공모가 대비 40% 빠지며 가장 큰 하락률을 보였다. 이 외에 와이즈넛(096250)(-36.47%), 아이지넷(462980)(-37.79%), 미트박스(475460)(-25.26%), 피아이이(-12.70%) 등도 두 자릿수 하락률을 기록했다.
올해 처음 코스피 시장에 상장한 LG CNS도 IPO 한파를 이겨내지 못했다. LG CNS는 상장 첫날 공모가(6만 1900원) 대비 6100원(9.85%) 내린 5만 5800원에 거래를 마쳤다. LG CNS의 주가는 이날 종가 기준 5만 4500원으로 8거래일 새 공모가 대비 10% 넘게 하락했다.
"""
inputs = tokenizer(input_text, return_tensors="pt", truncation=True, max_length=512)
inputs = {k: v.to(device) for k, v in inputs.items()}

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

summary = tokenizer.decode(output_ids[0], skip_special_tokens=True)
print("=== 요약 결과 ===")
print(summary)

def get_max_pooled_embedding(text):
    """
    주어진 텍스트를 토큰화한 후, 모델의 encoder 출력을 max pooling하여
    고정 길이 임베딩 벡터를 반환합니다.
    """
    # 토크나이징: 최대 길이 512 (필요에 따라 조정)
    encoded_input = tokenizer(text, return_tensors="pt", truncation=True, max_length=512)
    encoded_input = {k: v.to(device) for k, v in encoded_input.items()}
    
    with torch.no_grad():
        # mT5 모델의 encoder에 입력
        encoder_outputs = model.encoder(
            input_ids=encoded_input["input_ids"],
            attention_mask=encoded_input["attention_mask"]
        )
        # 마지막 은닉 상태: (batch_size, seq_length, hidden_size)
        last_hidden_states = encoder_outputs.last_hidden_state

    # attention mask: (batch_size, seq_length, 1)을 encoder 출력 크기로 확장
    mask = encoded_input["attention_mask"].unsqueeze(-1).expand(last_hidden_states.size()).bool()
    
    # 패딩 위치에는 매우 작은 값(-1e9)을 할당하여 max pooling 시 무시되도록 함
    masked_hidden_states = last_hidden_states.masked_fill(~mask, -1e9)

    # 시퀀스 차원에 대해 max pooling → (batch_size, hidden_size)
    pooled_vector = masked_hidden_states.max(dim=1).values
    return pooled_vector

def get_mean_pooled_embedding(text):
    """
    주어진 텍스트를 토큰화한 후, 모델의 encoder 출력을 mean pooling하여
    고정 길이 임베딩 벡터를 반환합니다.
    """
    # 토크나이징: 최대 길이 512 (필요에 따라 조정)
    encoded_input = tokenizer(text, return_tensors="pt", truncation=True, max_length=512)
    encoded_input = {k: v.to(device) for k, v in encoded_input.items()}
    
    with torch.no_grad():
        # mT5 모델의 encoder에 입력
        encoder_outputs = model.encoder(
            input_ids=encoded_input["input_ids"],
            attention_mask=encoded_input["attention_mask"]
        )
        # 마지막 은닉 상태: (batch_size, seq_length, hidden_size)
        last_hidden_states = encoder_outputs.last_hidden_state

    # attention mask를 float형으로 확장: (batch_size, seq_length, hidden_size)
    mask = encoded_input["attention_mask"].unsqueeze(-1).expand(last_hidden_states.size()).float()

    # 각 토큰에 해당하는 은닉 상태에 mask를 곱해준 후, 시퀀스 차원에 대해 합산
    sum_embeddings = torch.sum(last_hidden_states * mask, dim=1)
    # mask의 합 (각 샘플마다 실제 토큰의 개수)
    sum_mask = mask.sum(dim=1).clamp(min=1e-9)  # 0으로 나누는 것을 방지
    # 평균값 계산: (batch_size, hidden_size)
    pooled_vector = sum_embeddings / sum_mask
    return pooled_vector

# 6. 임베딩 추출 테스트
example_text = "이 문장은 고정 길이의 벡터로 변환될 것입니다."

# max pooling 임베딩 테스트
embedding_vector_max = get_max_pooled_embedding(example_text)
print("Max pooling을 적용한 임베딩 벡터의 크기:", embedding_vector_max.shape)

# mean pooling 임베딩 테스트
embedding_vector_mean = get_mean_pooled_embedding(example_text)
print("Mean pooling을 적용한 임베딩 벡터의 크기:", embedding_vector_mean.shape)