import numpy as np
from PIL import Image

def preProcessImage(img, image_size=(160, 160)):
    
    if isinstance(img, np.ndarray):
        img = Image.fromarray(img)

    # Convertir a escala de grises si el modelo fue entrenado con imágenes en blanco y negro
    img = img.convert('L')  # 'L' para convertir a escala de grises (ajusta según tu modelo)
    
    # Redimensionar la imagen
    img = img.resize(image_size)    

    # Convertir a array numpy
    img_array = np.array(img) / 255.0
    
    # Asegurarse de que la imagen tenga la forma correcta
    img_array = np.expand_dims(img_array, axis=-1)  # Añadir el canal (para escala de grises)
    img_array = np.expand_dims(img_array, axis=0)   # Añadir la dimensión del batch

    return img_array