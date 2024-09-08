
from confluent_kafka import Consumer,Producer, KafkaException, KafkaError
from PIL import Image
import io
import pandas as pd
from tensorflow.keras.models import load_model
import numpy as np
import json
import sys

from extractFace import extractFace
from preProcessImage import preProcessImage
from deliveryReport import deliveryReport


def main():
    print("Starting message consumer")
    modelo_cargado = load_model(r'C:\Users\Ruben\Documents\TFM_Local\PruebasTFM\model.h5')

    conf = {
        'bootstrap.servers': '192.168.68.110:9092', 
        'group.id': 'imageGroup',
        'auto.offset.reset': 'earliest',
        'session.timeout.ms': 10000,  # 10 segundos
        'heartbeat.interval.ms': 3000  # 3 segundos
    }

    confSender = {
        'bootstrap.servers': '192.168.68.110:9092'  # Cambia esto si tu Kafka está en otro host
        # 'linger.ms': 0,
        # 'batch.num.messages': 1
    } 

    consumer = Consumer(conf)
    producer = Producer(confSender)

    try:    
        consumer.subscribe(['imageTopic'])

        while True: 
      
            msg = consumer.poll(1)  # Esperar 1 segundo por mensajes

            if msg is None:
                # print("No message received in this poll cycle.")
                continue

        
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    raise KafkaException(msg.error())
            
            # Procesar el mensaje
            img_bytes = msg.value()
            
            # Verificar el tamaño de los bytes
            print(f"Received image bytes of length: {len(img_bytes)}")
            
            # Intentar abrir la imagen
            faces = []
        
            image = Image.open(io.BytesIO(img_bytes))  

            consumer.commit(msg)  # Confirmar el offset del mensaje procesado
            
            faces = extractFace(image)

            for face in faces:            
                imagen_procesada = preProcessImage(face, image_size=(160, 160))
                prediccion = modelo_cargado.predict(imagen_procesada)
                clase_predicha = np.argmax(prediccion, axis=1)
                probabilidad_clase_predicha = prediccion[0][clase_predicha[0]]*100;

                respuesta = {
                    'clase_predicha': int(clase_predicha[0]),  # Convertir a int
                    'probabilidad_clase_predicha': float(probabilidad_clase_predicha)  # Convertir a float
                }

                respuesta_json = json.dumps(respuesta, indent=4)

                producer.produce('imagePrediction', value=respuesta_json, callback=deliveryReport)
                producer.flush()            

                print(respuesta_json)
    except Exception as e:
        print(f"Error: {e}")
        consumer.close()
    

if __name__ == "__main__":
    main()

    
