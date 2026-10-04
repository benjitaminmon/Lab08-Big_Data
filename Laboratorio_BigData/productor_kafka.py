import json
import time
import random
from kafka import KafkaProducer
from datetime import datetime

# 1. Configuración del Productor de Kafka
def conectar_kafka():
    try:
        producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        return producer
    except Exception as e:
        print(f"Error al conectar con Kafka: {e}")
        return None

# 2. Simulación de Datos (Creatividad de dominio)
usuarios = ['Benjamin', 'Janiera', 'Berenice', 'Rayen', 'Esteban', 'Benji', 'Amy', 'Gerardini', 'Shanara', 'Javiera']
acciones = ['login', 'descarga_material', 'ver_clase_grabada', 'solicitud_ayudantia', 'logout']

def generar_evento():
    return {
        'timestamp': datetime.now().isoformat(),
        'usuario': random.choice(usuarios),
        'accion': random.choice(acciones),
        'tiempo_respuesta_ms': random.randint(50, 500)
    }

# 3. Bucle de Ingesta (1 evento por segundo como pide el instructivo)
def iniciar_simulacion():
    producer = conectar_kafka()
    if not producer:
        return

    print("Iniciando productor de Kafka... Presiona Ctrl+C para detener.")
    topic_name = 'logs-plataforma'

    try:
        while True:
            evento = generar_evento()
            producer.send(topic_name, value=evento)
            print(f"Enviado a Kafka -> {evento}")
            time.sleep(1) # Simula log cada segundo
    except KeyboardInterrupt:
        print("\nSimulación detenida por el usuario.")
    finally:
        producer.close()
        print("Conexión con Kafka cerrada.")

if __name__ == '__main__':
    iniciar_simulacion()