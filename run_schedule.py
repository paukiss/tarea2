import schedule
import time
import subprocess
from datetime import datetime

def run_spider():
    print(f"[{datetime.now()}] Ejecutando spider...")
    subprocess.run([
        "scrapy", "crawl", "newspaper_spider",
        "-o", f"data/salida_{datetime.now().strftime('%Y-%m-%d')}.json"
    ])

# Ejecutar cada 2 días
schedule.every(2).days.at("11:59").do(run_spider)


print("Scheduler iniciado. Presiona Ctrl+C para detener.")

while True:
    schedule.run_pending()
    time.sleep(60)
