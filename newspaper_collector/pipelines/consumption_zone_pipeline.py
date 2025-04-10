# consumption_zone_pipeline.py

from itemadapter import ItemAdapter
from urllib.parse import urlparse
import os
import psycopg2
from dotenv import load_dotenv
import logging
from dateutil import parser 
from datetime import date, time

class ConsumptionZonePipeline:

    def __init__(self):
        load_dotenv()
        hostname = os.getenv('DB_HOST')
        username = os.getenv('DB_USER')
        password = os.getenv('DB_PASSWORD')
        database = os.getenv('DB_DATABASE')
        port = os.getenv('DB_PORT')

        self.connection = None
        self.cur = None
        try:
            self.connection = psycopg2.connect(
                host=hostname,
                port=port,
                user=username,
                password=password,
                dbname=database
            )
            self.cur = self.connection.cursor()

            # ----- MODIFICACIÓN 1: Estructura de la tabla -----
            # Cambiamos 'fecha_noticia DATE' por 'fecha_noticia DATE' y 'hora_noticia TIME'
            self.cur.execute("""
                CREATE TABLE IF NOT EXISTS consumption_analytics (
                    id SERIAL PRIMARY KEY,
                    titulo TEXT,
                    fecha_noticia DATE,       -- Columna para la fecha
                    hora_noticia TIME,        -- Columna para la hora
                    seccion TEXT,
                    fuente TEXT,
                    url TEXT UNIQUE,
                    fecha_procesado TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                );
            """)
            # Asegurar índices (el de fecha ya existe, podríamos añadir hora)
            self.cur.execute("CREATE INDEX IF NOT EXISTS idx_consumption_url ON consumption_analytics(url);")
            self.cur.execute("CREATE INDEX IF NOT EXISTS idx_consumption_fecha ON consumption_analytics(fecha_noticia);")
            # self.cur.execute("CREATE INDEX IF NOT EXISTS idx_consumption_hora ON consumption_analytics(hora_noticia);") # Opcional
            self.cur.execute("CREATE INDEX IF NOT EXISTS idx_consumption_fuente ON consumption_analytics(fuente);")
            self.connection.commit()
            logging.info("Pipeline de Consumo: Conexión a BD establecida y tabla asegurada (con fecha/hora).")
            # ----- FIN MODIFICACIÓN 1 -----

        except psycopg2.Error as e:
            logging.error(f"Pipeline de Consumo: Error al conectar o crear tabla: {e}")
            self.connection = None
            self.cur = None

    def open_spider(self, spider):
        if not self.connection:
             spider.logger.error("Pipeline de Consumo: No se pudo establecer conexión con la BD en __init__.")

    def close_spider(self, spider):
        if self.cur:
            self.cur.close()
        if self.connection:
            self.connection.close()
            logging.info("Pipeline de Consumo: Conexión a BD cerrada.")

    def process_item(self, item, spider):
        if not self.cur or not self.connection:
            spider.logger.error(f"Pipeline de Consumo: Omitiendo item por falta de conexión a BD: {item.get('url')}")
            return item

        adapter = ItemAdapter(item)
        url_item = adapter.get('url')

        try:
            self.cur.execute("SELECT id FROM consumption_analytics WHERE url = %s", (url_item,))
            result = self.cur.fetchone()
            if result:
                spider.logger.debug(f"Pipeline de Consumo: Item ya existe en Consumption Zone: {url_item}")
                return item
        except psycopg2.Error as e:
            spider.logger.error(f"Pipeline de Consumo: Error al verificar duplicado para {url_item}: {e}")
            return item

        try:
            fuente = self.extract_fuente(url_item)

            fecha_str = adapter.get('fecha') 
            fecha_noticia_obj = None
            hora_noticia_obj = None

            if fecha_str:
                try:
                    parsed_datetime = parser.parse(fecha_str)
                    fecha_noticia_obj = parsed_datetime.date() 
                    hora_noticia_obj = parsed_datetime.time()  
                except (ValueError, TypeError, OverflowError, parser.ParserError) as e:
                    spider.logger.warning(f"Pipeline de Consumo: No se pudo parsear fecha/hora desde '{fecha_str}' para {url_item}: {e}. Se guardará como NULL.")
                    fecha_noticia_obj = None
                    hora_noticia_obj = None

            self.cur.execute("""
                INSERT INTO consumption_analytics
                (titulo, fecha_noticia, hora_noticia, seccion, fuente, url)
                VALUES (%s, %s, %s, %s, %s, %s) -- refined_id puesto a NULL
                ON CONFLICT (url) DO NOTHING;
            """, (
                adapter.get('titulo'),
                fecha_noticia_obj, 
                hora_noticia_obj, 
                adapter.get('seccion'),
                fuente,
                url_item
            ))
            self.connection.commit()
            if self.cur.rowcount > 0:
                 spider.logger.info(f"Pipeline de Consumo: Item insertado en Consumption Zone: {url_item}")

        except psycopg2.Error as e:
            spider.logger.error(f"Pipeline de Consumo: Error al insertar item {url_item}: {e}")
            self.connection.rollback()
        except Exception as e:
            spider.logger.error(f"Pipeline de Consumo: Error inesperado procesando {url_item}: {e}", exc_info=True)
            try:
                self.connection.rollback()
            except psycopg2.Error as rb_err:
                 spider.logger.error(f"Pipeline de Consumo: Error durante rollback: {rb_err}")

        return item

    # --- Funciones auxiliares ---
    def extract_fuente(self, url):
        if not url: return None
        try:
            domain = urlparse(url).netloc
            if 'eldeber.com.bo' in domain: return 'eldeber'
            elif 'lostiempos.com' in domain: return 'lostiempos'
            elif 'ahoraelpueblo.bo' in domain: return 'ahoraelpueblo'
            else:
                parts = domain.split('.')
                if len(parts) >= 2: return parts[-2] if parts[-2] != 'com' else parts[0]
                return domain
        except Exception: return None
