import os
import cv2
import pandas as pd
from pyzbar.pyzbar import decode
from PIL import Image
import pytesseract


class ProcesadorGuiaOCR:
    """
    Procesa una imagen de guía logística:
    - Extrae número de guía desde el nombre del archivo
    - Detecta código de barras (opcional)
    - Detecta DEVOLUCIONES vía OCR
    - Acumula resultados en un DataFrame
    """

    def __init__(self, ruta_tesseract):
        pytesseract.pytesseract.tesseract_cmd = ruta_tesseract

        self.df_resultados = pd.DataFrame(columns=[
            "ruta_imagen",
            "numero_guia",
            "codigo_barras",
            "tiene_devolucion",
            "estado_devolucion",
            "tiene_codigo_barras"
        ])

        pd.set_option('display.max_colwidth', None)

    # -------------------------------------------------

    @staticmethod
    def _extraer_numero_guia(image_path):
        nombre = os.path.basename(image_path)
        guia = os.path.splitext(nombre)[0]
        return int(guia) if guia.isdigit() else guia

    # -------------------------------------------------

    @staticmethod
    def _tiene_devoluciones(texto):
        if not texto:
            return False
        texto = texto.lower()
        palabras = ["devoluciones", "devolucion", "devoluci", "devoluc", "DEVOL","DEVOLUCIONES", "DEVOLU", "DEVOLUCION","Devoluciones","DEVOLUCIÓN"]
        return any(p in texto for p in palabras)

    # -------------------------------------------------

    @staticmethod
    def _ocr_tesseract(cv_image):
        gray = cv2.cvtColor(cv_image, cv2.COLOR_BGR2GRAY)

        clahe = cv2.createCLAHE(clipLimit=2.0, tileGridSize=(8, 8))
        enhanced = clahe.apply(gray)

        thresh = cv2.adaptiveThreshold(
            enhanced, 255,
            cv2.ADAPTIVE_THRESH_GAUSSIAN_C,
            cv2.THRESH_BINARY, 11, 2
        )

        config = r'--oem 3 --psm 6 -l spa'
        return pytesseract.image_to_string(thresh, config=config).strip()

    # -------------------------------------------------

    def procesar_imagen(self, image_path):
        """
        Procesa una imagen y devuelve el DataFrame acumulado
        """
        i_error = 0
        try:
            cv_image = cv2.imread(image_path)
            if cv_image is None:
                raise Exception("No se pudo cargar la imagen")

            numero_guia = self._extraer_numero_guia(image_path)

            # ---------------- Barcode ----------------
            gray = cv2.cvtColor(cv_image, cv2.COLOR_BGR2GRAY)
            _, thresh = cv2.threshold(
                gray, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU
            )

            pil_original = Image.fromarray(cv2.cvtColor(cv_image, cv2.COLOR_BGR2RGB))
            pil_thresh = Image.fromarray(thresh)

            decoded = decode(pil_original) or decode(pil_thresh)
            codigo_barras = (
                decoded[0].data.decode('utf-8', errors='ignore')
                if decoded else None
            )

            # ---------------- OCR ----------------
            texto_ocr = self._ocr_tesseract(cv_image)

            tiene_dev = self._tiene_devoluciones(texto_ocr)

            # ---------------- DataFrame ----------------
            self.df_resultados.loc[len(self.df_resultados)] = {
                "ruta_imagen": image_path,
                "numero_guia": numero_guia,
                "codigo_barras": codigo_barras,
                "tiene_devolucion": tiene_dev,
                "estado_devolucion": "SÍ" if tiene_dev else "NO",
                "tiene_codigo_barras": codigo_barras is not None
            }

            #print(f"✅ Procesada guía {numero_guia}")

        except Exception as e:
            #print(f"❌ Error procesando imagen: {e}")
            i_error = i_error +1
        return self.df_resultados,i_error
