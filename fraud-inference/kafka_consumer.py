from kafka import KafkaConsumer
import requests
import json
import os
import time

BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = "fraud_transactions"
PREDICT_API = "http://fraud-inference:8060/predict" # sửa app thành fraud-inference

def main():
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=[BOOTSTRAP_SERVERS],
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        # Thay đổi group_id để kafka consumer luôn đọc từ offset đầu tiên KHI CHƯA CÓ OFFSET MỚI
        group_id="fraud-group" + str(time.time())
    )

    for msg in consumer:
        transaction = msg.value
        res = requests.post(PREDICT_API, json=transaction)
        print(f"🔎 Transaction {transaction.get('TransactionID')} => {res.json()}")

if __name__ == "__main__":
    try:
        res = requests.get(PREDICT_API)
        if res.status_code == 200:
            print("API sẵn sàng")
    except requests.exceptions.RequestException:
        print("API chưa sẵn sàng, thử lại sau 2s...")
        time.sleep(2)
    main()
