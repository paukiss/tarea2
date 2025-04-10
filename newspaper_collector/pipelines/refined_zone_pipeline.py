# pipelines.py

from itemadapter import ItemAdapter
from datetime import datetime
from dateutil import parser
import emoji
import re
from urllib.parse import urlparse
import os
import psycopg2
from dotenv import load_dotenv

class RefinedZonePipeline:

    def __init__(self):
        load_dotenv()  
        hostname = os.getenv('DB_HOST')
        username = os.getenv('DB_USER')
        password = os.getenv('DB_PASSWORD')
        database = os.getenv('DB_DATABASE')
        port = os.getenv('DB_PORT')
        

        self.connection = psycopg2.connect(
            host=hostname,
            port=port,
            user=username,
            password=password,
            dbname=database
        )
        self.cur = self.connection.cursor()

        self.cur.execute("""
            CREATE TABLE IF NOT EXISTS newspaper (
                id SERIAL PRIMARY KEY,
                data_id TEXT,
                titulo TEXT,
                descripcion TEXT,
                fecha TEXT,
                seccion TEXT,
                url TEXT,
                date_saved_iso TEXT
            );
        """)
        self.connection.commit()

    def process_item(self, item, spider):
        """
        Recibe un NewspaperItem con campos:
          - data_id
          - titulo
          - descripcion
          - fecha
          - seccion
          - url
          - date_saved
        Aplica transformaciones y lo guarda en la BD.
        """
        # Convertir item a diccionario
        transformed = dict(item)

        for key, value in transformed.items():
            if isinstance(value, str):
                transformed[key] = value.lower().strip()

        for field in transformed:
            if transformed[field] == "" or transformed[field] == " " or transformed[field] == "null":
                transformed[field] = None

        if 'descripcion' in transformed and transformed['descripcion']:
            if isinstance(transformed['descripcion'], list):
                desc = ' '.join(transformed['descripcion'])
            else:
                desc = transformed['descripcion']

            desc = self.clean_text(desc)
            desc = self.remove_links(desc)
            transformed['descripcion'] = desc

        if 'fecha' in transformed and transformed['fecha']:
            try:
                parsed_date = parser.parse(transformed['fecha'])
                transformed['fecha'] = parsed_date.isoformat()
            except (ValueError, TypeError):
                transformed['fecha'] = transformed['fecha']  

        if 'date_saved' in transformed and transformed['date_saved']:
            try:
                dt = datetime.fromisoformat(transformed['date_saved'])
                transformed['date_saved'] = dt.isoformat()
            except:
                transformed['date_saved'] = None
        else:
            transformed['date_saved'] = None

        # 6) Validar que el título no esté vacío
        if not transformed.get('titulo'):
            spider.logger.error("Título vacío: no se guarda la noticia.")
            raise Exception("Título vacío: no se guarda la noticia.")

        # 7) Verificar si ya existe esa url en la BD
        if 'url' in transformed and transformed['url']:
            self.cur.execute("""
                SELECT id FROM newspaper
                WHERE url = %s
            """, (transformed['url'],))
            res = self.cur.fetchone()
        else:
            res = None

        # Actualizar el item con los valores transformados
        adapter = ItemAdapter(item)
        for key, value in transformed.items():
            adapter[key] = value

        # ... (código anterior) ...

        if res: # Si la URL ya existe
            spider.logger.info(f"NOTICIA YA EXISTE EN BD: {transformed['url']}")
            return item
        else: # Si la URL NO existe, intentar insertar
            # ----- INICIO DEL BLOQUE PROBLEMÁTICO -----
            try:
                self.cur.execute("""
                    INSERT INTO newspaper (
                        data_id, titulo, descripcion, fecha, seccion, url, date_saved_iso
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (
                    transformed.get('data_id'),
                    transformed.get('titulo'),
                    transformed.get('descripcion'),
                    transformed.get('fecha'),
                    transformed.get('seccion'),
                    transformed.get('url'),
                 
                    transformed.get('date_saved')
                ))
                self.connection.commit() 
                spider.logger.debug(f"RefinedZonePipeline: Item insertado: {transformed.get('url')}")
            except psycopg2.Error as e:
                 spider.logger.error(f"RefinedZonePipeline: Error psycopg2 al INSERTAR {transformed.get('url')}: {e}")
          
                 self.connection.rollback()

            except Exception as e:
                 spider.logger.error(f"RefinedZonePipeline: Error inesperado al INSERTAR {transformed.get('url')}: {e}", exc_info=True)
            
                 try:
                     self.connection.rollback()
                 except psycopg2.Error as rb_err:
                     spider.logger.error(f"RefinedZonePipeline: Error durante rollback tras excepción: {rb_err}")

            return item 



    def close_spider(self, spider):
        self.cur.close()
        self.connection.close()


    def clean_text(self, text):
        if not text:
            return text
        text = re.sub("[\U00010000-\U0010ffff]", "", text, flags=re.UNICODE)
        text = re.sub(r'[^a-zA-Z0-9\s]', '', text)
        text = re.sub(r'\s+', ' ', text)
        return text.strip()

    def remove_links(self, text):
        if not text:
            return text

        url_pattern = r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|' \
                      r'(?:%[0-9a-fA-F][0-9a-fA-F]))+'
        text = re.sub(url_pattern, '', text)
        text = re.sub(r'www\.[^\s]+', '', text)
        return text.strip()

    def extract_domain(self, url):
        try:
            parsed_url = urlparse(url)
            domain = parsed_url.netloc
            return domain
        except:
            return None

