import schedule
import time
import subprocess
from datetime import datetime
import logging # Importar logging

# Configurar logging básico para ver qué está haciendo el scheduler
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def run_etl_process():
    """Ejecuta el spider de Scrapy y luego el script para poblar la Consumption Zone."""
    logging.info(f"[{datetime.now()}] Iniciando proceso ETL completo...")

    # 1. Ejecutar el Scrapy Spider
    logging.info("Ejecutando spider (Scrapy)...")
    try:
        # Usamos check=True para que lance un error si Scrapy falla
        # capture_output=True para capturar stdout/stderr y mostrarlos si hay error
        result_scrapy = subprocess.run(
            ["scrapy", "crawl", "newspaper_spider"],
            check=True,
            capture_output=True,
            text=True,
            encoding='utf-8' # Especificar encoding
        )
        logging.info("Spider (Scrapy) finalizado exitosamente.")
        # Opcional: mostrar salida estándar de Scrapy si es útil
        # logging.debug(f"Salida Scrapy:\n{result_scrapy.stdout}")

    except subprocess.CalledProcessError as e:
         logging.error(f"Error durante la ejecución del spider (Scrapy): {e}")
         logging.error(f"Salida del error Scrapy:\n{e.stderr}")
         # Decidir si continuar o no si el spider falla
         # return # Podrías detener el proceso aquí si el spider es crítico

    except FileNotFoundError:
        logging.error("Error: El comando 'scrapy' no se encontró. ¿Está Scrapy instalado y en el PATH?")
        return # Detener si scrapy no se encuentra
    except Exception as e:
         logging.error(f"Error inesperado ejecutando Scrapy: {e}")
         return # Detener en caso de error inesperado


    # 2. Ejecutar el script para poblar la Consumption Zone (SI USAS EL SCRIPT SEPARADO)
    # Si implementaste la Consumption Zone como pipeline, puedes comentar/eliminar esta parte.
    logging.info("Ejecutando script para poblar Consumption Zone...")
    try:
        # Asegúrate de que 'python' ejecute la versión correcta y que el script esté en la ruta
        result_populate = subprocess.run(
            ["python", "populate_consumption.py"],
             check=True,
             capture_output=True,
             text=True,
             encoding='utf-8' # Especificar encoding
        )
        logging.info("Script de Consumption Zone finalizado exitosamente.")
        # Opcional: mostrar salida estándar del script
        # logging.debug(f"Salida Script Consumo:\n{result_populate.stdout}")

    except subprocess.CalledProcessError as e:
         logging.error(f"Error durante la ejecución del script populate_consumption.py: {e}")
         logging.error(f"Salida del error Script Consumo:\n{e.stderr}")
    except FileNotFoundError:
         logging.error("Error: El comando 'python' o el script 'populate_consumption.py' no se encontró.")
    except Exception as e:
         logging.error(f"Error inesperado ejecutando populate_consumption.py: {e}")


    logging.info(f"[{datetime.now()}] Proceso ETL completo finalizado.")


# --- Programación de la Tarea ---
# Comenta/elimina cualquier línea de prueba (ej. every(10).seconds)
# schedule.every(10).seconds.do(run_etl_process)

# Descomenta y usa UNA de las siguientes líneas para la ejecución cada 2 días:
# Opción 1: Cada 2 días (la hora exacta dependerá de cuándo se inicie el scheduler)
# schedule.every(2).days.do(run_etl_process)

# Opción 2: Cada 2 días A UNA HORA ESPECÍFICA (ej. 03:00 AM) - Recomendado
schedule.every(2).days.at("03:00").do(run_etl_process)

logging.info("Scheduler ETL iniciado. Programado para ejecutarse cada 2 días a las 03:00. Presiona Ctrl+C para detener.")

# Bucle principal para mantener el scheduler corriendo
while True:
    schedule.run_pending() # Comprueba si hay tareas pendientes de ejecutar
    time.sleep(60) # Espera 60 segundos antes de volver a comprobar (no necesita ser cada segundo)