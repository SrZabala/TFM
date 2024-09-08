from PIL import Image
import numpy as np 
from mtcnn.mtcnn import MTCNN
import cv2


def extractFace(image, size=(160, 160)):
    
    detector = MTCNN()
        
    imageRGB = image.convert('RGB')

    pixelsImage = np.asarray(imageRGB)    
    facesDetected = detector.detect_faces(pixelsImage)

    if not facesDetected:
        raise ValueError("No face detected in the image.")

    faces = []
    
    for face in facesDetected:
        x1, y1, width, height = face['box']
        x1, y1 = abs(x1), abs(y1)
        x2, y2 = x1 + width, y1 + height

        face = pixelsImage[y1:y2, x1:x2]
        faceImage = Image.fromarray(face)
        faceImageResize = faceImage.resize(size)
        faceArray = np.asarray(faceImageResize)
        
        face = cv2.cvtColor(faceArray, cv2.COLOR_RGB2GRAY)

        faces.append(face)
        
    return np.asarray(faces)            
