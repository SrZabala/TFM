import base64
from datetime import datetime, timedelta
from io import BytesIO
import json
from confluent_kafka import Consumer, Producer, KafkaException, KafkaError
import matplotlib.pyplot as plt
import numpy as np
from pymongo import MongoClient

# Configuración del consumidor Kafka
conf = {
    'bootstrap.servers': 'localhost:9092',  # Servidor de Kafka
    'group.id': 'imageGroup',  # Grupo de consumidores
    'auto.offset.reset': 'earliest',  # Empieza a leer desde el principio si no hay offset guardado
    'session.timeout.ms': 10000,  # Timeout de la sesión (10 segundos)
    'heartbeat.interval.ms': 3000  # Intervalo del heartbeat (3 segundos)
}

# Configuración del productor Kafka
confSender = {
    'bootstrap.servers': 'localhost:9092'  # Servidor de Kafka para el productor
}

def fig_to_base64(fig):
    """Convierte una figura de matplotlib a una cadena base64."""
    buf = BytesIO()  # Crear un buffer en memoria
    fig.savefig(buf, format='png')  # Guardar la figura en formato PNG en el buffer
    buf.seek(0)  # Volver al principio del buffer
    return base64.b64encode(buf.read()).decode('utf-8')  # Convertir el contenido a base64

def generate_statistics_figures(db):
    """Genera gráficos de estadísticas a partir de datos en MongoDB."""
    # Configuración del tamaño de las etiquetas en los gráficos
    plt.rcParams['xtick.labelsize'] = 14  # Tamaño de la fuente para el eje X
    plt.rcParams['ytick.labelsize'] = 14  # Tamaño de la fuente para el eje Y

    # 1. Crear un diagrama de barras con la cantidad de clases
    collection = db['Estadisticas']  # Colección de estadísticas en MongoDB
    consulta = [
        {'$group': {'_id': '$Clase', 'count': {'$sum': 1}}},  # Agrupar por clase y contar ocurrencias
        {'$sort': {'count': -1}}  # Ordenar por cantidad de mayor a menor
    ]
    results = list(collection.aggregate(consulta))  # Ejecutar la consulta
    classes = [result['_id'] for result in results]  # Obtener los nombres de las clases
    counts = [result['count'] for result in results]  # Obtener las cantidades

    # Crear la figura del gráfico de barras
    fig = plt.figure(figsize=(10, 6))
    bars = plt.bar(classes, counts, color='skyblue')  # Crear las barras del gráfico
    plt.title('Número de veces que aparece cada Clase')  # Título del gráfico
    plt.xticks(rotation=45, fontsize=14)  # Rotar etiquetas del eje X
    plt.yticks(fontsize=14)  # Ajustar el tamaño de las etiquetas del eje Y
    plt.tight_layout()  # Ajustar el gráfico para que todo se vea correctamente
    plt.gca().axes.get_yaxis().set_visible(False)  # Ocultar el eje Y

    # Añadir etiquetas sobre cada barra con el valor
    for bar in bars:
        yval = bar.get_height()
        plt.text(bar.get_x() + bar.get_width() / 2, yval, int(yval), ha='center', va='bottom', fontsize=14)

    # Convertir el gráfico de barras a base64
    clasificacionesUltimos7Dias = fig_to_base64(fig)
    plt.close(fig)  # Cerrar la figura para liberar memoria

    # 2. Crear un gráfico de líneas con el número de clasificaciones por día en los últimos 7 días
    end_date = datetime.utcnow()  # Fecha y hora actual
    start_date = end_date - timedelta(days=7)  # Fecha de hace 7 días
    consulta = [
        {'$addFields': {
            'fechaHora': {'$dateFromString': {'dateString': '$fechaHora', 'format': '%Y-%m-%d %H:%M:%S'}}
        }},  # Convertir la fecha a formato adecuado
        {'$match': {'fechaHora': {'$gte': start_date, '$lt': end_date}}},  # Filtrar datos de los últimos 7 días
        {'$project': {'date': {'$dateToString': {'format': '%Y-%m-%d', 'date': '$fechaHora'}}}},  # Extraer solo la fecha
        {'$group': {'_id': '$date', 'count': {'$sum': 1}}},  # Agrupar por fecha y contar clasificaciones
        {'$sort': {'_id': 1}}  # Ordenar por fecha
    ]
    results = list(collection.aggregate(consulta))  # Ejecutar la consulta
    dates = [result['_id'] for result in results]  # Obtener las fechas
    counts = [result['count'] for result in results]  # Obtener el conteo de clasificaciones por día

    # Crear la figura del gráfico de líneas
    fig = plt.figure(figsize=(12, 6))
    plt.plot(dates, counts, marker='o', linestyle='-', color='skyblue', linewidth=2, markersize=8)  # Crear la línea
    plt.title('Número de Clasificaciones por Día en los Últimos 7 Días')  # Título del gráfico
    plt.xticks(rotation=45, fontsize=14)  # Rotar etiquetas del eje X
    plt.yticks(fontsize=14)  # Ajustar el tamaño de las etiquetas del eje Y
    plt.grid(True)  # Mostrar cuadrícula
    plt.tight_layout()  # Ajustar el gráfico
    plt.gca().axes.get_yaxis().set_visible(False)  # Ocultar el eje Y

    # Añadir etiquetas de conteo encima de cada punto en la gráfica
    for date, count in zip(dates, counts):
        plt.text(date, count, str(count), ha='center', va='bottom', fontsize=14)

    # Convertir el gráfico de líneas a base64
    numClasificacionesPorSujeto = fig_to_base64(fig)
    plt.close(fig)  # Cerrar la figura para liberar memoria

    # Retornar los gráficos en formato base64
    return clasificacionesUltimos7Dias, numClasificacionesPorSujeto

def main():
    """Función principal que consume mensajes de Kafka, genera estadísticas y las envía de vuelta a Kafka."""
    print("Starting updateDeleteSubjects")
    consumer = Consumer(conf)  # Crear el consumidor de Kafka
    producer = Producer(confSender)  # Crear el productor de Kafka

    try:
        consumer.subscribe(['giveStatistics'])  # Suscribirse al tópico 'giveStatistics'

        while True:
            msg = consumer.poll(1)  # Esperar 1 segundo por nuevos mensajes

            if msg is None:
                continue  # Si no hay mensajes, continuar esperando

            if msg.error():
                print(f"Consumer error: {msg.error()}")
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue  # Fin de la partición alcanzada
                else:
                    raise KafkaException(msg.error())  # Otros errores

            # Leer el mensaje recibido y convertirlo a un valor booleano
            boolExtract = np.frombuffer(msg.value(), dtype=bool)[0]
            consumer.commit(msg)  # Confirmar que el mensaje fue procesado

            if boolExtract:  # Si el mensaje contiene True, generar estadísticas
                client = MongoClient('mongodb://localhost:27017/')  # Conexión a MongoDB
                db = client['TFM']  # Seleccionar base de datos 'TFM'

                # Generar gráficos y estadísticas
                clasificacionesUltimos7Dias, numClasificacionesPorSujeto = generate_statistics_figures(db)

                # Obtener estadísticas adicionales de la base de datos
                collection = db['Caracteristicas']
                numSujetos = collection.count_documents({})  # Contar el número de sujetos
                collection = db['DataImages']
                numImagenesTotales = collection.count_documents({})  # Contar el número total de imágenes

                # Calcular la mediana del número de imágenes por sujeto
                consulta = [
                    {'$group': {'_id': '$class', 'count': {'$sum': 1}}},
                    {'$sort': {'count': 1}}
                ]
                results = list(collection.aggregate(consulta))
                counts = [result['count'] for result in results]
                numImagenesMedianaSujeto = np.median(counts)

                # Calcular clasificaciones correctas e incorrectas
                collection = db['Estadisticas']
                clasificacinesCorrectas = collection.count_documents({'probabilidad_clase_predicha': {'$gte': 90}})
                clasificacinesIncorrectas = collection.count_documents({'probabilidad_clase_predicha': {'$lt': 90}})

                # Calcular la probabilidad media de clasificación
                conculta = [
                    {'$group': {'_id': None, 'average_probability': {'$avg': '$probabilidad_clase_predicha'}}}
                ]
                result = collection.aggregate(conculta)
                mediaProbabilidadClasificacion = next(result, {}).get('average_probability', None)

                # Crear un objeto con los resultados
                objeto = {
                    'numSujetos': numSujetos,
                    'numImagenesTotales': numImagenesTotales,
                    'numImagenesMedianaSujeto': numImagenesMedianaSujeto,
                    'clasificacinesCorrectas': clasificacinesCorrectas,
                    'clasificacinesIncorrectas': clasificacinesIncorrectas,
                    'mediaProbabilidadClasificacion': mediaProbabilidadClasificacion,
                    'numClasificacionesPorSujeto': numClasificacionesPorSujeto,
                    'clasificacionesUltimos7Dias': clasificacionesUltimos7Dias
                }

                # Convertir el objeto a JSON y enviarlo a Kafka
                json_resultado = json.dumps(objeto, indent=4)
                print(json_resultado)
                producer.produce('Statistics', json_resultado.encode('utf-8'))  # Producir mensaje en Kafka
                producer.flush()  # Asegurar que el mensaje fue enviado
                print("Subjects sent to Kafka")

    except Exception as e:
        print(f"Error: {e}")
    finally:
        consumer.close()  # Cerrar el consumidor de Kafka

if __name__ == "__main__":
    main()  # Ejecutar el programa principal