from kafka import KafkaProducer
import time
import os

# 1. Configurar el productor apuntando a tu Docker local
producer = KafkaProducer(bootstrap_servers='localhost:9092')
topic = 'agro_raw'

print("Conectado a Kafka. Iniciando simulación de streaming...")

# 2. Ruta al archivo real (usamos uno de los CSV)
archivo = 'extraido/dataset-grecia/kompoti_soil-moisture-30cm_openhi_10min_2015-2026.csv'

if not os.path.exists(archivo):
    print(f"Error: No se encuentra el archivo {archivo}. Verifica la ruta.")
else:
    with open(archivo, 'r') as f:
        for i, line in enumerate(f):
            if i >= 15: # Enviamos solo 15 registros para la demo
                break
            
            # Enviamos la línea CSV tal cual a Kafka
            producer.send(topic, line.encode('utf-8'))
            print(f"Enviado a Kafka [topic: {topic}]: {line.strip()}")
            time.sleep(0.3) # Simula la llegada cada 300ms

    producer.flush()
    print("\nSimulación terminada. ¡Datos en Kafka!")
