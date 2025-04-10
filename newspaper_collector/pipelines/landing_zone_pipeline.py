# pipelines.py

from itemadapter import ItemAdapter
from datetime import datetime
import os
import json

class LandingZonePipeline:
    def __init__(self):
        self.landing_zone_dir = 'datalake/LANDING_ZONE'
        os.makedirs(self.landing_zone_dir, exist_ok=True)
        self.file = None

    def open_spider(self, spider):
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"landing_data_{spider.name}_{timestamp}.jsonl" 
        filepath = os.path.join(self.landing_zone_dir, filename)
        self.file = open(filepath, 'a', encoding='utf-8')
        spider.logger.info(f"Abriendo archivo de Landing Zone: {filepath}")

    def close_spider(self, spider):
        if self.file:
            self.file.close()
            spider.logger.info("Archivo de Landing Zone cerrado.")

    def process_item(self, item, spider):
     
        try:
            adapter = ItemAdapter(item)
            line = json.dumps(adapter.asdict(), ensure_ascii=False) + "\n"
            self.file.write(line)
        except Exception as e:
            spider.logger.error(f"Error al escribir en Landing Zone: {e}")
        return item
    

