import numpy as np
from PIL import Image
import io


def image_to_bytes(image_array):
    # Eliminar dimensiones adicionales
    image_array = np.squeeze(image_array)
    
    # Convertir el array de NumPy a una imagen de PIL
    img = Image.fromarray(image_array)
    
    # Crear un buffer de bytes
    byte_arr = io.BytesIO()
    
    # Guardar la imagen en el buffer de bytes en formato PNG
    img.save(byte_arr, format='PNG')
    
    # Obtener los bytes del buffer
    byte_arr = byte_arr.getvalue()
    
    return byte_arr