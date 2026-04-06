import json
from kafka import KafkaProducer
from websocket import WebSocketApp

KAFKA_TOPIC = "crypto_events"
KAFKA_BROKER = "localhost:9092"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def on_open(ws):
    print("Connected to Coinbase WebSocket")

    subscribe_message = {
        "type": "subscribe",
        "channels": [
            {"name": "matches", "product_ids": ["BTC-USD"]}
        ]
    }

    ws.send(json.dumps(subscribe_message))

def on_message(ws, message):
    data = json.loads(message)

    if data.get("type") == "match":  # trade event
        event = {
            "symbol": data.get("product_id"),
            "price": data.get("price"),
            "size": data.get("size"),
            "timestamp": data.get("time")
        }

        print("Sending:", event)
        producer.send(KAFKA_TOPIC, event)

def on_error(ws, error):
    print("ERROR:", error)

def on_close(ws, close_status_code, close_msg):
    print("Connection closed")

if __name__ == "__main__":
    socket = "wss://ws-feed.exchange.coinbase.com"

    ws = WebSocketApp(
        socket,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )

    ws.run_forever()