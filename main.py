from procesador_guia_ocr import ProcesadorGuiaOCR
from conexion_sql import get_connection
from conexion_sql import get_sqlalchemy_engine
import pandas as pd
from datetime import datetime

# Validar  disponibilidad de ruta \\servient.com.co\imgmasnal\IMGMASNAL\NACIONAL\02-05-2021\2025\05\12\2\

class main:
    def __init__(self):
        hora_inical = datetime.now()
        
    def EjecucionPaso01_Devoluciones(): 
        conexion = get_connection()
        if conexion:
            try:
                consulta_script = f'''
                                    SELECT 
                                        NUMERO_GUIA,
                                        PATH_IMAGEN
                                        FROM  [TMP].[TEMU_GUIAS_DEVOLUCIONES_01] WITH(NOLOCK) 
                                        WHERE NOMBRE_ZONA_URBA <> 'D RF VOLANTES SUR 20'
                                        AND NUMERO_GUIA NOT IN (SELECT numero_guia from [TMP].[TEMU_GUIAS_DEVOLUCIONES_02] WITH(NOLOCK))
                                        AND PATH_IMAGEN IS NOT NULL
                                        OPTION(FAST 1)                                     
                                        '''
                df_SP02 = pd.read_sql(consulta_script, conexion)
                           
                print("Procedimiento ejecutado correctamente.")
                print(datetime.now())
            except Exception as e:
                print("Error al ejecutar la consulta:", e)
            finally:
                print("Conexión cerrada correctamente.")
            return(df_SP02)    

if __name__ == "__main__":
    RUTA_TESSERACT = r'\\t_serv-dbi01\T\App_Costos_Darwin\Analizador_IMG\tesseract.exe'
    
    errores = 0 
    guias = main.EjecucionPaso01_Devoluciones()    
    
    numero_registros = len(guias)  

    i = 1 
    guias_df = pd.DataFrame(columns=[
    "ruta_imagen",
    "numero_guia",
    "codigo_barras",
    "tiene_devolucion",
    "estado_devolucion",
    "tiene_codigo_barras"
        ])
    
    for indice, fila in guias.iterrows():
        GUIA_PATH = fila['PATH_IMAGEN']
        # print(GUIA_PATH)        
        #GUIA_PATH = r'\\servient.com.co\imgmasnal\IMGMASNAL\NACIONAL\02-05-2021\2025\06\18\2\045\2247178045.TIF'

        try:
            procesador = ProcesadorGuiaOCR(RUTA_TESSERACT)
            df,errores = procesador.procesar_imagen(GUIA_PATH)
            df['numero_guia'] = fila['NUMERO_GUIA']
        except:
            errores = errores + 1
        finally:
            if len(df)>=1:               
                guias_df = pd.concat([guias_df, df], ignore_index=True)        
            i = i+1
        print(f"\rRegistros procesados: {i} / {numero_registros}   \U0000274C CON PRESENCIA DE ERRORES  {errores}", end="", flush=True)


    # print(len(guias_df))
    print('\n\n')
    print(f'total registros generados con error: {numero_registros}')
    print('\n\n')
    
    # Truncar tabla de [TMP].[TEMU_GUIAS_DEVOLUCIONES_02]
    conexion = get_connection()
    if conexion:
        try:
            cursor = conexion.cursor()
            consulta_SP = '''
                                EXEC [TMP].[TEMU_SP_GUIAS_DEVOLUCIONES_02]
                        '''
            cursor.execute(consulta_SP) 
            # Asegurar que Python consuma todos los result sets y espere hasta el final
            ct_rs = 0
            while cursor.nextset():
                ct_rs = 1+ct_rs
                print("Ejecucion numero de result sets: "+str(ct_rs))    
                pass
            print("Script ejecutado correctamente.")
            print(datetime.now())
            conexion.commit()    
        except Exception as e:
            print("Error al ejecutar la consulta:", e)
        finally:
            cursor.close()
            conexion.close()
            print("Conexión cerrada correctamente.")

    print(f'Numero de errores {errores}')

    guias_df = guias_df[pd.to_numeric(guias_df['numero_guia'], errors='coerce').notna()]

    #INSERTAR DATOS EN TABLA DE PASO CON CALCULO DE ESTADISTICOS
    batch_size = 5000
    conexion = get_connection()
    if conexion:
        try:
            conexion.autocommit = True  # Mejora la velocidad eliminando commits explícitos
            cursor = conexion.cursor()
            cursor.fast_executemany = True  # Activa inserción rápida en lotes
            
            data = [tuple(row) for row in guias_df.itertuples(index=False, name=None)]
            total_rows = len(data)

            sql = '''INSERT INTO [TMP].[TEMU_GUIAS_DEVOLUCIONES_02] WITH (TABLOCK)
                    VALUES (?,	?,	?,	?,	?,	?)'''

            for i in range(0, total_rows, batch_size):
                batch = data[i:min(i + batch_size, total_rows)]
                cursor.executemany(sql, batch)  # Se ejecuta sin necesidad de commit explícito
                
                porcentaje = int((i + len(batch)) / total_rows * 100)
                print(f"\r{porcentaje}% Insertados {i + len(batch)} de {total_rows} registros...", end="", flush=True)

            print("\nInserción completada correctamente.")  
            print(datetime.now())
        except Exception as e:
            print("\nError al ejecutar la consulta:", e)
        finally:
            # cursor.close()
            # conexion.close()
            print("Conexión cerrada correctamente.")    