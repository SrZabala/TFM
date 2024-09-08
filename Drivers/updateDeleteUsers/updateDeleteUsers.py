import json
from confluent_kafka import Consumer, Producer, KafkaException, KafkaError
import numpy as np
from pymongo import MongoClient

# Configuración del consumidor Kafka
conf = {
    'bootstrap.servers': '192.168.68.110:9092',  # Dirección del servidor de Kafka
    'group.id': 'imageGroup',  # Identificador del grupo de consumidores
    'auto.offset.reset': 'earliest',  # Leer mensajes desde el principio si no hay offset guardado
    'session.timeout.ms': 10000,  # Tiempo de espera para detectar consumidores inactivos (10 segundos)
    'heartbeat.interval.ms': 3000  # Intervalo para enviar un latido (heartbeat) y mantener la conexión viva (3 segundos)
}

def main():
    print("Starting updateDeleteSubjects")
    
    # Crear un consumidor Kafka para recibir mensajes del tópico 'editSubjects'
    consumer = Consumer(conf)

    try:
        # Suscribirse al tópico 'editSubjects'
        consumer.subscribe(['editSubjects'])

        while True:  # Bucle continuo para procesar mensajes de Kafka
            msg = consumer.poll(1)  # Esperar 1 segundo para recibir mensajes

            # Si no se recibe ningún mensaje, continuar esperando
            if msg is None:
                continue

            # Verificar si el mensaje contiene algún error
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue  # Si se alcanza el final de la partición, continuar
                else:
                    raise KafkaException(msg.error())  # Si hay otro error, lanzar una excepción

            # Convertir el mensaje recibido (en formato JSON) a una cadena
            json_string = msg.value()

            # Confirmar que se ha procesado el mensaje
            consumer.commit(msg)

            # Convertir la cadena JSON en un diccionario de Python
            user_data = json.loads(json_string)

            # Extraer los campos relevantes del mensaje JSON
            Clase = user_data.get("Clase")  # Nombre de la clase a actualizar o eliminar
            Nombre = user_data.get("Nombre")  # Nombre de la persona
            Apellido = user_data.get("Apellido")  # Apellido de la persona
            Descripcion = user_data.get("Descripcion")  # Nueva descripción para la clase
            Operation = user_data.get("Operacion")  # Tipo de operación: "Update" o "Delete"

            # Conectar a la base de datos MongoDB
            client = MongoClient('mongodb://localhost:27017/')
            db = client['TFM']  # Acceder a la base de datos 'TFM'
            
            # Acceder a las colecciones relevantes
            collectionCaracteristicas = db['Caracteristicas']  # Colección que almacena las características de las clases
            collectionDataImages = db['DataImages']  # Colección que almacena las imágenes y etiquetas

            # Si la operación es una actualización ("Update")
            if Operation == "Update":
                # Actualizar la descripción de la clase en la colección Caracteristicas
                result = collectionCaracteristicas.update_one(
                    {"Clase": Clase},  # Filtro: encontrar el documento con la clase especificada
                    {"$set": {"Descripcion": Descripcion}}  # Acción: establecer una nueva descripción
                )
                
                # Informar si el documento fue actualizado exitosamente
                if result.matched_count > 0:
                    print(f"Documento con Clase '{Clase}' actualizado con nueva Descripcion.")
                else:
                    print(f"No se encontró documento con Clase '{Clase}'.")

            # Si la operación es eliminar ("Delete")
            elif Operation == "Delete":
                # Eliminar el documento correspondiente a la clase en la colección Caracteristicas
                result = collectionCaracteristicas.delete_one({"Clase": Clase})
                
                # Informar si el documento fue eliminado exitosamente
                if result.deleted_count > 0:
                    print(f"Documento con Clase '{Clase}' eliminado.")
                else:
                    print(f"No se encontró documento con Clase '{Clase}'.")

                # Eliminar todas las imágenes correspondientes a la clase en la colección DataImages
                result_data_images = collectionDataImages.delete_many({"class": Clase})
                
                # Informar si se eliminaron las imágenes correspondientes a la clase
                if result_data_images.deleted_count > 0:
                    print(f"Documentos con Clase '{Clase}' eliminados de la colección DataImages.")
                else:
                    print(f"No se encontraron documentos con Clase '{Clase}' en la colección DataImages.")

            # Si la operación no es reconocida
            else:
                print("Operación no reconocida")

    except Exception as e:
        # Manejo de excepciones y cierre del consumidor en caso de error
        print(f"Error: {e}")
        consumer.close()

if __name__ == "__main__":
    main()