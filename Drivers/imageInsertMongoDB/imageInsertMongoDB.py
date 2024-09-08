import base64
from confluent_kafka import Consumer, Producer, KafkaException, KafkaError
from PIL import Image
import io
import pandas as pd
import numpy as np
import json
import sys

# Importar funciones personalizadas para extracción y procesamiento de caras, y manejo de Kafka
from Class.extractFace import extractFace
from Class.preProcessImage import preProcessImage
from Class.deliveryReport import deliveryReport
from pymongo import MongoClient
import bson

def main():
    """Función principal para consumir mensajes de Kafka, procesar imágenes y almacenarlas en MongoDB."""
    print("Starting message consumer")  # Mensaje para indicar que el consumidor se ha iniciado

    # Configuración del consumidor Kafka
    conf = {
        'bootstrap.servers': 'localhost:9092',  # Dirección del servidor Kafka
        'group.id': 'imageGroup',  # ID del grupo de consumidores
        'auto.offset.reset': 'earliest',  # Empieza desde el inicio del log si no hay offset guardado
        'session.timeout.ms': 10000,  # Tiempo de espera de la sesión en milisegundos (10 segundos)
        'heartbeat.interval.ms': 3000  # Intervalo del latido del consumidor en milisegundos (3 segundos)
    }

    # Configuración del productor Kafka (aunque está comentada porque no se usa en este código)
    confSender = {
        'bootstrap.servers': 'localhost:9092'  # Dirección del servidor Kafka para el productor
        # 'linger.ms': 0,
        # 'batch.num.messages': 1
    } 

    # Crear el consumidor Kafka con la configuración definida
    consumer = Consumer(conf)
    # Crear un productor Kafka (aunque está comentado en este caso)
    # producer = Producer(confSender)

    # Suscribirse al tópico 'insertMongo', que contiene mensajes que se insertarán en MongoDB
    consumer.subscribe(['insertMongo'])

    # Bucle infinito para procesar continuamente los mensajes recibidos
    while True: 
        # try:

        # Esperar hasta 1 segundo por un nuevo mensaje desde Kafka
        msg = consumer.poll(1)

        # Si no se recibió ningún mensaje, continuar esperando
        if msg is None:
            continue

        # Manejo de errores si el mensaje tiene algún problema
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue  # Si se alcanzó el final de la partición, continuar
            else:
                raise KafkaException(msg.error())  # Lanza una excepción si es otro error

        # Procesar el mensaje decodificándolo desde bytes a una cadena UTF-8
        message = msg.value().decode('utf-8')
        
        # Intentar cargar el mensaje como un JSON
        try:
            json_data = json.loads(message)
        except json.JSONDecodeError as e:
            print(f"JSON decode error: {e}")  # Si hay un error en el formato JSON, lo imprime y continúa
            continue

        # Extraer los datos del JSON recibido: Nombre, Apellido, Descripción e Imagen codificada en Base64
        nombre = json_data.get("Nombre")
        apellido = json_data.get("Apellido")
        clase = nombre + "_" + apellido  # Crear la clase combinando nombre y apellido
        descripcion = json_data.get("Descripcion")
        imagen_base64 = json_data.get("ImagenBase64")
        
        # Decodificar la imagen que está en Base64 para convertirla a bytes
        imagen_bytes = base64.b64decode(imagen_base64)
        
        # Intentar abrir la imagen desde los bytes usando la librería PIL
        faces = []  # Inicializar una lista para almacenar las caras detectadas
        image = Image.open(io.BytesIO(imagen_bytes))  # Convertir los bytes a una imagen de PIL

        # Confirmar que el mensaje fue procesado correctamente (confirmar el offset)
        consumer.commit(msg)
        
        # Extraer las caras de la imagen utilizando una función personalizada
        faces = extractFace(image)

        # Procesar cada cara detectada
        for face in faces:  
            # Preprocesar la imagen de la cara para que sea compatible con otros sistemas (ajustar tamaño, etc.)
            imagen_procesada = preProcessImage(face, image_size=(160, 160))
            
            # Conectar a la base de datos MongoDB
            client = MongoClient('mongodb://localhost:27017/')
            db = client['TFM']  # Acceder a la base de datos 'TFM'

            # Insertar la imagen en la colección 'DataImages' en formato binario BSON
            collection = db['DataImages']
            image_bytes = bson.binary.Binary(imagen_procesada)  # Convertir la imagen a formato BSON

            # Verificar si la imagen ya existe en la base de datos para evitar duplicados
            if collection.find_one({'imageBytes': image_bytes}) is None:
                document = {
                    'class': clase,  # Clase (nombre + apellido)
                    'imageBytes': image_bytes,  # Imagen en formato binario
                }
                collection.insert_one(document)  # Insertar la imagen en la colección
                print("Image inserted in DataImages")  # Confirmar que la imagen se insertó
            else:
                print("Image already exists in DataImages")  # Si ya existe, mostrar un mensaje

            # Insertar las características del sujeto en la colección 'Caracteristicas'
            collection = db['Caracteristicas']
            # Verificar si la clase ya existe en la base de datos
            if collection.find_one({'Clase': clase}) is None:
                document = {
                    'Clase': clase,  # Clase (nombre + apellido)
                    'Nombre': nombre,  # Nombre del sujeto
                    'Apellido': apellido,  # Apellido del sujeto
                    'Descripcion': descripcion,  # Descripción del sujeto
                }
                collection.insert_one(document)  # Insertar las características en la base de datos
                print("Caracteristicas inserted in MongoDB")  # Confirmar la inserción
            else:
                print("Clase already exists in Caracteristicas")  # Si ya existe, mostrar un mensaje

if __name__ == "__main__":
    # Ejecutar la función principal si el script es ejecutado directamente
    main()