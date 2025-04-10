# pipelines.py

from itemadapter import ItemAdapter
from datetime import datetime
import os
import json

class LandingZonePipeline:
    def __init__(self):
        # Directorio para la Landing Zone
        self.landing_zone_dir = 'datalake/LANDING_ZONE'
        # Crear el directorio si no existe
        os.makedirs(self.landing_zone_dir, exist_ok=True)
        self.file = None

    def open_spider(self, spider):
        # Crear un nombre de archivo único con fecha y hora
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"landing_data_{spider.name}_{timestamp}.jsonl" # Usamos .jsonl para JSON Lines
        filepath = os.path.join(self.landing_zone_dir, filename)
        # Abrir el archivo en modo append ('a') para escribir línea por línea
        self.file = open(filepath, 'a', encoding='utf-8')
        spider.logger.info(f"Abriendo archivo de Landing Zone: {filepath}")

    def close_spider(self, spider):
        # Cerrar el archivo cuando el spider termine
        if self.file:
            self.file.close()
            spider.logger.info("Archivo de Landing Zone cerrado.")

    def process_item(self, item, spider):
        # Esta pipeline recibe el item casi directamente del spider.
        # NO aplicamos limpieza profunda aquí, solo lo necesario para guardar.
        # Convertimos el item a un diccionario
        try:
            adapter = ItemAdapter(item)
            line = json.dumps(adapter.asdict(), ensure_ascii=False) + "\n"
            self.file.write(line)
        except Exception as e:
            spider.logger.error(f"Error al escribir en Landing Zone: {e}")
        # Devolvemos el item SIN MODIFICAR para que pase a la siguiente pipeline
        return item
    

