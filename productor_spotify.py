import time
import json
import random
from kafka import KafkaProducer

generos =["Pop", "Rap", "Rock", "EDM", "Reggaeton"]

def generar_evento_spotify():
    return {
        "user_id": random.randint(1000, 9999),
        "genre": random.choice(generos),
        "listen_duration": round(random.uniform(30.0, 240.0), 2),
        "timestamp": int(time.time())
    }

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: json.dumps(x).encode('utf-8'))

print("Iniciando simulador de streams de Spotify...")
while True:
    evento = generar_evento_spotify()
    producer.send('spotify_streams', value=evento)
    print(f"Evento enviado: {evento}")
    time.sleep(1)
