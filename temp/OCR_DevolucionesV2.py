import os
import cv2
import numpy as np
import requests
from io import BytesIO
from datetime import datetime
from pyzbar.pyzbar import decode
from PIL import Image
import pytesseract

class OCR_DevolucionesV2:
    def __init__(self):
        # Rutas corporativas (Ajustar si cambian)
        self.tesseract_exe = r'\\t_serv-dbi01\T\App_Costos_Darwin\Analizador_IMG\Tesseract-OCR\Tesseract-OCR\tesseract.exe'
        self.tessdata_dir = r'\\t_serv-dbi01\T\App_Costos_Darwin\Analizador_IMG\Tesseract-OCR\Tesseract-OCR\tessdata'
        
        pytesseract.pytesseract.tesseract_cmd = self.tesseract_exe
        os.environ['TESSDATA_PREFIX'] = self.tessdata_dir

    def tiene_devoluciones(self, texto):
        if not texto: return False
        palabras_clave = ["devoluciones", "devolucion", "devoluci", "devoluc", "remitente"]
        return any(p in texto.lower() for p in palabras_clave)

    def ocr_tesseract(self, image_path, cv_image=None):
        try:
            if cv_image is None:
                cv_image = cv2.imread(image_path)
            gray = cv2.cvtColor(cv_image, cv2.COLOR_BGR2GRAY)
            _, thresh = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
            config = f'--oem 3 --psm 6 -l spa --tessdata-dir "{self.tessdata_dir}"'
            return pytesseract.image_to_string(thresh, config=config).strip()
        except: return ""

    def procesar_guia_unica(self, source):
        """ PROCESA UNA SOLA GUÍA (URL O RUTA) """
        try:
            if source.startswith(('http://', 'https://')):
                response = requests.get(source, timeout=15)
                pil_img = Image.open(BytesIO(response.content))
            else:
                pil_img = Image.open(source)

            # Procesar
            cv_img = cv2.cvtColor(np.array(pil_img), cv2.COLOR_RGB2BGR)
            barcode = "No detectado"
            decoded = decode(pil_img)
            if decoded: barcode = decoded[0].data.decode('utf-8')
            
            texto = self.ocr_tesseract(None, cv_img)
            dev = self.tiene_devoluciones(texto)

            return {
                "OK": True,
                "Barcode": barcode,
                "Devolucion": "SÍ" if dev else "NO",
                "OCR_Preview": texto[:50].replace('\n', ' ')
            }
        except Exception as e:
            return {"OK": False, "Error": str(e)}

# --- EJEMPLO DE USO ---
if __name__ == "__main__":
    app = OCR_DevolucionesV2()

    # 1. Probar con RUTA LOCAL
    ruta_local = r'D:\Dev_Proyectos\Dev_Py_Darwin\leer_imagenes\Fotos_Entrada\2247178045.TIF'
    resultado1 = app.procesar_guia_unica(ruta_local)
    print(f"Resultado Local: {resultado1}")

    # 2. Probar con URL
    # url_guia = r"\\servient.com.co\imgmasnal\IMGMASNAL\NACIONAL\02-05-2021\2025\06\18\2\045\2247178045.TIF"
    # resultado2 = app.procesar_guia_unica(url_guia)
    # print(f"Resultado URL: {resultado2}")