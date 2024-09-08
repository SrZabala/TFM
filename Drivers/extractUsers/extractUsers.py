import json
from confluent_kafka import Consumer, Producer, KafkaException, KafkaError
import numpy as np
from pymongo import MongoClient

# Configuración del consumidor Kafka
conf = {
    'bootstrap.servers': 'localhost:9092',  # Dirección del servidor Kafka
    'group.id': 'imageGroup',  # Identificación del grupo de consumidores
    'auto.offset.reset': 'earliest',  # Empieza a consumir desde el principio si no hay offset guardado
    'session.timeout.ms': 10000,  # Tiempo de espera de la sesión en milisegundos (10 segundos)
    'heartbeat.interval.ms': 3000  # Intervalo de envío de heartbeat en milisegundos (3 segundos)
}

# Configuración del productor Kafka
confSender = {
    'bootstrap.servers': 'localhost:9092'  # Dirección del servidor Kafka para el productor
    # 'linger.ms': 0,  # Configuración opcional para optimizar el envío de mensajes
    # 'batch.num.messages': 1  # Número de mensajes por lote (si quieres enviar varios a la vez)
} 

def main():
    """Función principal que escucha mensajes de Kafka, consulta MongoDB y envía los resultados a Kafka."""
    print("Starting updateDeleteSubjects")
    
    # Crear un consumidor Kafka con la configuración dada
    consumer = Consumer(conf)
    
    # Crear un productor Kafka con la configuración dada
    producer = Producer(confSender)

    try:
        # Suscribirse al tópico 'giveMeSubjects' para recibir mensajes
        consumer.subscribe(['giveMeSubjects'])

        while True:
            # Esperar hasta 1 segundo por un nuevo mensaje
            msg = consumer.poll(1)

            if msg is None:
                # Si no se recibió ningún mensaje, continuar esperando
                continue

            # Manejar errores del consumidor
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # Fin de la partición alcanzada, continuar leyendo mensajes
                    continue
                else:
                    # Si hay otro error, lanzar una excepción
                    raise KafkaException(msg.error())

            # Convertir el mensaje recibido (en formato binario) a un valor booleano
            boolExtract = np.frombuffer(msg.value(), dtype=bool)[0]

            # Confirmar que el mensaje fue procesado
            consumer.commit(msg)

            # Si el mensaje contiene un valor True, proceder a extraer datos de MongoDB
            if boolExtract:
                # Conectar a MongoDB en 'localhost' (puerto predeterminado: 27017)
                client = MongoClient('mongodb://localhost:27017/')
                
                # Seleccionar la base de datos 'TFM'
                db = client['TFM']
                
                # Seleccionar la colección 'Caracteristicas'
                collection = db['Caracteristicas']
                
                # Obtener todos los documentos de la colección
                documents = collection.find()

                # Lista para almacenar los documentos extraídos
                lista_documentos = []

                # Recorrer cada documento en la colección
                for document in documents:
                    # Crear un objeto con los campos deseados
                    objeto = {
                        'Clase': document['Clase'],  # Clase del sujeto
                        'Nombre': document['Nombre'],  # Nombre del sujeto
                        'Apellido': document['Apellido'],  # Apellido del sujeto
                        'Descripcion': document['Descripcion']  # Descripción del sujeto
                    }
                    # Añadir el objeto a la lista de documentos
                    lista_documentos.append(objeto)

                # Convertir la lista de objetos a formato JSON
                json_resultado = json.dumps(lista_documentos, indent=4)

                # Mostrar el JSON generado en la consola
                print(json_resultado)

                # Enviar el JSON al tópico 'totalSubjects' en Kafka
                producer.produce('totalSubjects', json_resultado.encode('utf-8'))

                # Esperar hasta que el mensaje se haya enviado
                producer.flush()

                # Confirmar que los sujetos fueron enviados a Kafka
                print("Subjects sent to Kafka")

    except Exception as e:
        # En caso de error, imprimirlo y cerrar el consumidor
        print(f"Error: {e}")
        consumer.close()

if __name__ == "__main__":
    # Ejecutar la función principal cuando se ejecute el script
    main()