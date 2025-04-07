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
import json

class NewspaperCollectorPipeline:

    def __init__(self):
        load_dotenv()  # Por si necesitas cargar variables de entorno .env
        # Información de conexión con la base de datos
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

        # Creamos la tabla (si no existe) adaptada a tus campos actuales:
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

        # 1) Convertir todos los campos string a minúsculas
        for key, value in transformed.items():
            if isinstance(value, str):
                transformed[key] = value.lower().strip()

        # 2) Convertir cadenas vacías en None
        for field in transformed:
            if transformed[field] == "" or transformed[field] == " " or transformed[field] == "null":
                transformed[field] = None

        # 3) Limpiar texto en "descripcion" (remover emojis, links, etc.)
        if 'descripcion' in transformed and transformed['descripcion']:
            # Si descripción llegó como lista, convertirla a un solo string.
            if isinstance(transformed['descripcion'], list):
                desc = ' '.join(transformed['descripcion'])
            else:
                desc = transformed['descripcion']

            # Quitar emojis y caracteres no alfanuméricos
            desc = self.clean_text(desc)
            # Remover links
            desc = self.remove_links(desc)
            transformed['descripcion'] = desc

        # 4) Convertir "fecha" a ISO8601 si posible
        #    (Algunos artículos tienen formatos de fecha raros, es opcional)
        if 'fecha' in transformed and transformed['fecha']:
            try:
                parsed_date = parser.parse(transformed['fecha'])
                transformed['fecha'] = parsed_date.isoformat()
            except (ValueError, TypeError):
                transformed['fecha'] = transformed['fecha']  # Mantén el valor original

        # 5) Manejo de date_saved => date_saved_iso
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

        if res:
            spider.logger.info(f"NOTICIA YA EXISTE EN BD: {transformed['url']}")
            # Permitimos que el ítem continúe hacia el JSON
            return item
        else:
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
                transformed.get('date_saved_iso')
            ))
            self.connection.commit()
            return item



    def close_spider(self, spider):
        """Cierra la conexión al terminar."""
        self.cur.close()
        self.connection.close()

    # --------------------------------------------------------------------------
    #                     FUNCIONES AUXILIARES
    # --------------------------------------------------------------------------

    def clean_text(self, text):
        """
        Ejemplo simple de limpieza de emojis y todo lo que no sea
        letras, números o espacios.
        """
        if not text:
            return text
        # Quitar emojis usando un regex, luego quitar caracteres no alfanuméricos
        # excepto espacios:
        # 1) Quita emojis (usando range Unicode de Emoticons) - a veces no basta
        text = re.sub("[\U00010000-\U0010ffff]", "", text, flags=re.UNICODE)
        # 2) Quita todo lo que no sea letras (a-z, A-Z), números o espacios
        text = re.sub(r'[^a-zA-Z0-9\s]', '', text)
        # Normaliza espacios múltiples a uno solo
        text = re.sub(r'\s+', ' ', text)
        return text.strip()

    def remove_links(self, text):
        """
        Remueve patrones de URL típicos.
        """
        if not text:
            return text

        # Patrón básico de URL
        url_pattern = r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|' \
                      r'(?:%[0-9a-fA-F][0-9a-fA-F]))+'
        text = re.sub(url_pattern, '', text)
        text = re.sub(r'www\.[^\s]+', '', text)
        return text.strip()

    def extract_domain(self, url):
        """Extract domain name from URL"""
        try:
            parsed_url = urlparse(url)
            # Get domain with subdomain (e.g., trabajito.com.bo)
            domain = parsed_url.netloc
            return domain
        except:
            return None


class JsonWriterPipeline:
    def open_spider(self, spider):
        fecha = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        self.file = open(f'data/salida_{fecha}.json', 'w', encoding='utf-8')

    def close_spider(self, spider):
        self.file.close()

    def process_item(self, item, spider):
        line = json.dumps(ItemAdapter(item).asdict(), ensure_ascii=False) + "\n"
        self.file.write(line)
        return item
