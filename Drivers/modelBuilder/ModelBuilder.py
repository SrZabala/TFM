import base64
from io import BytesIO
import json
import tensorflow as tf
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Conv2D, MaxPooling2D, Flatten, Dense, Dropout
from tensorflow.keras.callbacks import ReduceLROnPlateau, EarlyStopping
from sklearn.model_selection import train_test_split
from tensorflow.keras.utils import to_categorical
from pymongo import MongoClient
from confluent_kafka import Consumer, Producer, KafkaException, KafkaError
import numpy as np
import matplotlib.pyplot as plt
from sklearn.metrics import confusion_matrix
import seaborn as sns

# Función que crea un modelo de red neuronal convolucional (CNN)
def CNNModel(imageShape, numClasses):
    # Construye una CNN secuencial
    model = Sequential([
        Input(shape=imageShape),  # La entrada tiene la forma de las imágenes (160x160x1)
        Conv2D(32, (3, 3), activation='linear'),  # Capa de convolución con 32 filtros y activación linear
        MaxPooling2D((2, 2)),  # Capa de max pooling para reducir la dimensionalidad
        Conv2D(64, (3, 3), activation='relu'),  # Segunda capa de convolución con 64 filtros y activación ReLU
        MaxPooling2D((2, 2)),  # Max pooling
        Conv2D(128, (3, 3), activation='relu'),  # Tercera capa de convolución con 128 filtros
        MaxPooling2D((2, 2)),  # Max pooling
        Flatten(),  # Aplanar el tensor 3D a 1D
        Dense(256, activation='relu'),  # Capa densa con 256 neuronas y activación ReLU
        Dropout(0.5),  # Dropout para prevenir el sobreajuste
        Dense(numClasses, activation='softmax')  # Capa de salida con tantas neuronas como clases, usando softmax
    ])
    # Compilar el modelo usando el optimizador Adam y la función de pérdida categorical_crossentropy
    model.compile(optimizer='adam', loss='categorical_crossentropy', metrics=['accuracy'])
    return model

# Función para convertir gráficos matplotlib en una cadena base64
def fig_to_base64(fig):
    buf = BytesIO()  # Crear un buffer para almacenar la imagen
    fig.savefig(buf, format='png')  # Guardar el gráfico en el buffer
    buf.seek(0)  # Volver al inicio del buffer
    return base64.b64encode(buf.read()).decode('utf-8')  # Codificar la imagen en base64 y retornarla como cadena

# Configuración para Kafka (consumidor)
conf = {
    'bootstrap.servers': 'localhost:9092',  # Dirección del servidor Kafka
    'group.id': 'imageGroup',  # Grupo de consumidores
    'auto.offset.reset': 'earliest',  # Leer mensajes desde el inicio si no hay offset guardado
    'session.timeout.ms': 10000,  # Tiempo de espera para detectar consumidores fallidos
    'heartbeat.interval.ms': 3000  # Intervalo de latido para verificar la conectividad
}

# Configuración para Kafka (productor)
confSender = {
    'bootstrap.servers': 'localhost:9092'  # Dirección del servidor Kafka para enviar mensajes
}

# Definir las dimensiones de las imágenes
imageShape = (160, 160, 1)  # Las imágenes son de 160x160 píxeles en escala de grises (1 canal)

def main():
    print("Starting model builder")

    # Crear un consumidor Kafka para recibir mensajes del tópico 'entrenamientoModelo'
    consumer = Consumer(conf)
    # Crear un productor Kafka para enviar resultados
    producer = Producer(confSender)

    try:
        # Suscribirse al tópico 'entrenamientoModelo'
        consumer.subscribe(['entrenamientoModelo'])

        while True:
            # Esperar 1 segundo para recibir mensajes
            msg = consumer.poll(1)

            if msg is None:
                continue  # Si no hay mensaje, continuar esperando

            # Verificar si hubo un error en el mensaje
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    raise KafkaException(msg.error())

            print(f"Nuevo mensaje")

            # Procesar el mensaje: Decodificar el mensaje como un array de booleanos
            boolTrainer = np.frombuffer(msg.value(), dtype=bool)[0]
            consumer.commit(msg)  # Confirmar que se ha procesado el mensaje

            # Si el mensaje indica que se debe entrenar el modelo
            if boolTrainer == True:
                # Conectar a MongoDB y acceder a la base de datos 'TFM'
                client = MongoClient('mongodb://localhost:27017/')
                db = client['TFM']

                # Acceder a la colección 'DataImages' que contiene imágenes
                collection = db['DataImages']
                documents = collection.find()  # Obtener todos los documentos de la colección

                # Listas para almacenar las imágenes y etiquetas
                loaded_images = []
                loaded_labels = []

                # Procesar cada documento (imagen y etiqueta)
                for document in documents:
                    image_bytes = document['imageBytes']  # Obtener los bytes de la imagen
                    image_array = np.frombuffer(image_bytes).reshape(imageShape)  # Convertir los bytes a un array numpy con la forma correcta
                    label = document['class']  # Obtener la clase/etiqueta de la imagen
                    loaded_images.append(image_array)  # Agregar la imagen a la lista
                    loaded_labels.append(label)  # Agregar la etiqueta a la lista

                # Convertir las listas en arrays numpy
                loaded_images = np.array(loaded_images)
                loaded_labels = np.array(loaded_labels)

                print("Imágenes cargadas:", loaded_images.shape)
                print("Etiquetas cargadas:", loaded_labels.shape)

                # Obtener el número de clases únicas
                numClasses = len(np.unique(loaded_labels))

                # Convertir las etiquetas en valores numéricos
                from sklearn.preprocessing import LabelEncoder
                labelEncoder = LabelEncoder()
                numLabels = labelEncoder.fit_transform(loaded_labels)

                # Preparar el JSON para insertar en MongoDB
                json_data = []
                for i, class_name in enumerate(labelEncoder.classes_):
                    json_data.append({
                        "index": i,
                        "Clase": class_name
                    })

                # Insertar en la colección 'TrazabilidadClasificaciones' de MongoDB
                collection = db['TrazabilidadClasificaciones']
                collection.delete_many({})  # Limpiar los documentos existentes
                if json_data:
                    collection.insert_many(json_data)
                    print("Datos insertados correctamente en la colección TrazabilidadClasificaciones.")
                else:
                    print("No hay datos para insertar.")

                # Convertir las etiquetas numéricas a formato one-hot encoding
                encodedLabels = to_categorical(numLabels)

                # Dividir los datos en entrenamiento y prueba (80% entrenamiento, 20% prueba)
                x_train, x_test, y_train, y_test = train_test_split(loaded_images, encodedLabels, test_size=0.2)

                # Crear el modelo CNN
                model = CNNModel(imageShape, numClasses)

                # Definir callbacks: EarlyStopping y ReduceLROnPlateau para ajustar el aprendizaje
                earlystop = EarlyStopping(monitor='val_loss', patience=10, restore_best_weights=True)
                reduce_lr = ReduceLROnPlateau(monitor='val_loss', factor=0.2, patience=5, min_lr=0.001)
                callbacks = [earlystop, reduce_lr]

                # Entrenar el modelo
                batch_size = 32
                epochs = 50
                history = model.fit(
                    x_train, y_train,
                    batch_size=batch_size,
                    epochs=epochs,
                    validation_data=(x_test, y_test),
                    callbacks=callbacks
                )

                # Evaluar el modelo en los datos de prueba
                loss, accuracy = model.evaluate(x_test, y_test, verbose=0)
                print(f'La precisión del modelo en los datos de prueba es del {accuracy * 100:.2f}%')

                # Guardar el modelo entrenado localmente
                model.save('C:\\Users\\Ruben\\Documents\\TFM_Local\\model.h5')

                # Crear gráficos de entrenamiento
                # Gráfico de precisión
                fig1, ax1 = plt.subplots()
                ax1.plot(history.history['accuracy'], label='Precisión en entrenamiento')
                ax1.plot(history.history['val_accuracy'], label='Precisión en validación')
                ax1.set_xlabel('Épocas')
                ax1.set_ylabel('Precisión')
                ax1.legend(loc='lower right')
                ax1.set_title('Precisión en entrenamiento y validación')
                precision_img = fig_to_base64(fig1)  # Convertir gráfico a base64
                plt.close(fig1)

                # Gráfico de pérdida
                fig2, ax2 = plt.subplots()
                ax2.plot(history.history['loss'], label='Pérdida en entrenamiento')
                ax2.plot(history.history['val_loss'], label='Pérdida en validación')
                ax2.set_xlabel('Épocas')
                ax2.set_ylabel('Pérdida')
                ax2.legend(loc='upper right')
                ax2.set_title('Pérdida en entrenamiento y validación')
                loss_img = fig_to_base64(fig2)  # Convertir gráfico a base64
                plt.close(fig2)

                # Matriz de confusión
                y_pred = model.predict(x_test)
                y_pred_classes = np.argmax(y_pred, axis=1)
                y_true = np.argmax(y_test, axis=1)
                conf_matrix = confusion_matrix(y_true, y_pred_classes)

                fig3, ax3 = plt.subplots(figsize=(10, 8))
                sns.heatmap(conf_matrix, annot=True, fmt='d', cmap='Blues', xticklabels=range(numClasses), yticklabels=range(numClasses), ax=ax3)
                ax3.set_xlabel('Clases predichas')
                ax3.set_ylabel('Clases verdaderas')
                ax3.set_title('Matriz de confusión')
                plt.xticks(fontsize=14)
                plt.yticks(fontsize=14)
                conf_matrix_img = fig_to_base64(fig3)  # Convertir gráfico a base64
                plt.close(fig3)

                # Preparar los resultados en formato JSON
                data = {
                    'loss': loss,
                    'accuracy': accuracy,
                    'precision_img': precision_img,
                    'loss_img': loss_img,
                    'conf_matrix_img': conf_matrix_img
                }

                # Convertir los resultados a JSON
                json_data = json.dumps(data)

                # Enviar los resultados a Kafka (tópico 'entrenamientoModeloResultados')
                producer.produce('entrenamientoModeloResultados', value=json_data)
                producer.flush()

    except Exception as e:
        print(f"Error: {e}")
        consumer.close()

if __name__ == "__main__":
    main()