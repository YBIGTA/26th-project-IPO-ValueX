import pandas as pd
from sklearn.preprocessing import StandardScaler
import Database.mongodb_connection as md
import pickle   
from huggingface_hub import hf_hub_download


def load_data_from_mongodb(collection_name):
    """ MongoDB에서 Project_IPO_ValueX 데이터베이스의 특정 컬렉션을 불러오는 함수 """
    client = md.mongo_client
    db = md.mongo_db
    collection = db[collection_name]
    data = pd.DataFrame(list(collection.find()))
    client.close()
    return data

def preprocess_data(df,name):
    """ 데이터 전처리 함수 """
    X=df[df['_id']==name].drop(columns=['_id','종가대비등락율','등락율_이진','등락율_구분'])
    exclude_columns = [
        'latent_1', 'latent_2', 'latent_3', 'latent_4', 'latent_5',
        'latent_6', 'latent_7', 'latent_8', 'latent_9', 'latent_10',
        'latent_11', 'latent_12', 'latent_13', 'latent_14', 'latent_15',
        'Growth', 'Infra', 'Protective', 'Sensitive'
    ]
    X_scaled=X.copy()
    columns_to_scale=[col for col in X.columns if col not in exclude_columns]
    scaler = StandardScaler()
    X_scaled[columns_to_scale] = scaler.fit_transform(X[columns_to_scale])
    return X_scaled

def load_xgboost_model():
    """ Hugging Face에서 XGBoost 모델을 다운로드하고 로드하는 함수 """
    try:
        model_path = hf_hub_download(repo_id="woojz/xgboost-classifier", filename="xgboost_binary.pkl")
        with open(model_path, "rb") as f:
            model = pickle.load(f)
        return model
    except Exception as e:
        print(f"❌ 모델 로드 오류: {e}")
        return None

def predict_with_model(model, X):
    preds = model.predict(X)
    return preds

def run_predict_neural_binary(name):
    # MongoDB에서 Project_IPO_ValueX 데이터 로드
    collection_name = "final_neural"  # 사용할 컬렉션 선택
    df = load_data_from_mongodb(collection_name)
    
    # 데이터 전처리
    X_scaled = preprocess_data(df,name)
    
    # Hugging Face 모델 로드
    model = load_xgboost_model()

    # 예측 수행
    predictions = predict_with_model(model, X_scaled)
    
    # 결과 출력
    return predictions