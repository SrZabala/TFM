import base64
from confluent_kafka import Consumer, Producer, KafkaException, KafkaError
from PIL import Image
import io
import pandas as pd
from pymongo import MongoClient
from tensorflow.keras.models import load_model
import numpy as np
import json
import sys

# Clases personalizadas para extraer caras, preprocesar imágenes y generar informes de entrega
from Class.extractFace import extractFace
from Class.preProcessImage import preProcessImage
from Class.deliveryReport import deliveryReport

# Configuración del consumidor Kafka
conf = {
    'bootstrap.servers': 'localhost:9092',  # Dirección del servidor Kafka
    'group.id': 'imageGroup',  # ID del grupo de consumidores
    'auto.offset.reset': 'earliest',  # Empieza desde el principio si no hay offset guardado
    'session.timeout.ms': 10000,  # Tiempo de espera de la sesión (10 segundos)
    'heartbeat.interval.ms': 3000  # Intervalo de latido del consumidor (3 segundos)
}

# Configuración del productor Kafka
confSender = {
    'bootstrap.servers': 'localhost:9092'  # Dirección del servidor Kafka para el productor
    # Configuraciones adicionales opcionales
    # 'linger.ms': 0,
    # 'batch.num.messages': 1
}

def main():
    """Función principal para consumir mensajes de Kafka, procesar imágenes y enviar predicciones."""
    print("Starting message consumer")

    # Crear un consumidor Kafka con la configuración dada
    consumer = Consumer(conf)
    
    # Crear un productor Kafka con la configuración dada
    producer = Producer(confSender)

    try:
        # Suscribirse al tópico 'imageTopic' para recibir mensajes con imágenes
        consumer.subscribe(['imageTopic'])

        while True:
            # Esperar hasta 1 segundo por un nuevo mensaje
            msg = consumer.poll(1)

            if msg is None:
                # Si no se recibe un mensaje en este ciclo, continuar esperando
                continue

            # Manejar errores en la recepción del mensaje
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # Si llega al final de la partición, continuar leyendo
                    continue
                else:
                    # Si es otro tipo de error, lanzar una excepción
                    raise KafkaException(msg.error())
            
            # Cargar el modelo de Keras previamente entrenado (ajustar la ruta según tu sistema)
            modelo_cargado = load_model('C:\\Users\\Ruben\\Documents\\TFM_Local\\model.h5')

            # Procesar el mensaje: leer los bytes de la imagen
            img_bytes = msg.value()

            # Imprimir el tamaño de los bytes recibidos
            print(f"Received image bytes of length: {len(img_bytes)}")
            
            # Intentar abrir la imagen desde los bytes recibidos
            faces = []  # Lista donde se guardarán las caras extraídas
            image = Image.open(io.BytesIO(img_bytes))  # Convertir los bytes a una imagen de PIL

            # Confirmar que el mensaje fue procesado
            consumer.commit(msg)
            
            # Extraer las caras de la imagen utilizando una función personalizada
            faces = extractFace(image)

            # Procesar cada cara detectada
            for face in faces:
                # Preprocesar la imagen de la cara para que sea compatible con el modelo
                imagen_procesada = preProcessImage(face, image_size=(160, 160))
                
                # Realizar la predicción usando el modelo cargado
                prediccion = modelo_cargado.predict(imagen_procesada)
                
                # Obtener la clase predicha (categoría con mayor probabilidad)
                clase_predicha = np.argmax(prediccion, axis=1)
                
                # Obtener la probabilidad asociada a la clase predicha
                probabilidad_clase_predicha = prediccion[0][clase_predicha[0]] * 100

                # Conectar a MongoDB para buscar información de las clases y estadísticas
                client = MongoClient('mongodb://localhost:27017/')
                db = client['TFM']
                
                # Buscar la clase predicha en la colección 'TrazabilidadClasificaciones'
                collection = db['TrazabilidadClasificaciones']
                clase_encontrada = collection.find_one({"index": int(clase_predicha[0])}, {"Clase": 1})

                # Buscar las características asociadas a la clase en la colección 'Caracteristicas'
                collection = db['Caracteristicas']
                Caracteristicas = collection.find_one({"Clase": clase_encontrada['Clase']}, {"Nombre": 1, "Apellido": 2, "Descripcion": 3})

                # Preparar la respuesta con la clase predicha y su información asociada
                respuesta = {
                    'Clase_Predicha': int(clase_predicha[0]),  # Índice de la clase predicha
                    'Clase': clase_encontrada['Clase'],  # Nombre de la clase
                    'Nombre': Caracteristicas['Nombre'],  # Nombre del sujeto
                    'Apellido': Caracteristicas['Apellido'],  # Apellido del sujeto
                    'Descripcion': Caracteristicas['Descripcion'],  # Descripción asociada
                    'probabilidad_clase_predicha': float(probabilidad_clase_predicha),  # Probabilidad de la predicción
                    'fechaHora': pd.Timestamp.utcnow().strftime("%Y-%m-%d %H:%M:%S")  # Fecha y hora de la predicción
                }

                # Convertir la respuesta a formato JSON
                respuesta_json = json.dumps(respuesta, indent=4)

                # Enviar la predicción al tópico 'imagePrediction' en Kafka
                producer.produce('imagePrediction', value=respuesta_json, callback=deliveryReport)
                producer.flush()  # Asegurarse de que el mensaje fue enviado

                # Insertar los resultados en la colección 'Estadisticas' en MongoDB
                collection = db['Estadisticas']
                collection.insert_one(respuesta)

                # Imprimir la respuesta en formato JSON
                print(respuesta_json)

    except Exception as e:
        # Manejar cualquier excepción que ocurra y cerrar el consumidor de Kafka
        print(f"Error: {e}")
        consumer.close()

if __name__ == "__main__":
    # Ejecutar la función principal cuando se inicie el script
    main()