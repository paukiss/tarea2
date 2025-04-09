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
import logging

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
    


class ConsumptionZonePipeline:

    def __init__(self):
        """Inicializa la conexión y crea la tabla de consumo si no existe."""
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

            # Crear la tabla consumption_analytics (misma estructura que antes)
            self.cur.execute("""
                CREATE TABLE IF NOT EXISTS consumption_analytics (
                    id SERIAL PRIMARY KEY,
                    refined_id INTEGER, -- No podemos garantizar FK si la pipeline anterior no devuelve el ID
                    titulo TEXT,
                    fecha_noticia DATE,
                    seccion TEXT,
                    fuente TEXT,
                    url TEXT UNIQUE,
                    fecha_procesado TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                );
            """)
            # Crear índices
            self.cur.execute("CREATE INDEX IF NOT EXISTS idx_consumption_url ON consumption_analytics(url);")
            self.cur.execute("CREATE INDEX IF NOT EXISTS idx_consumption_fecha ON consumption_analytics(fecha_noticia);")
            self.cur.execute("CREATE INDEX IF NOT EXISTS idx_consumption_fuente ON consumption_analytics(fuente);")
            self.connection.commit()
            logging.info("Pipeline de Consumo: Conexión a BD establecida y tabla asegurada.")

        except psycopg2.Error as e:
            logging.error(f"Pipeline de Consumo: Error al conectar o crear tabla: {e}")
            # Si falla la conexión inicial, esta pipeline no funcionará.
            self.connection = None
            self.cur = None

    def open_spider(self, spider):
        """Placeholder, la conexión se abre en __init__."""
        if not self.connection:
             spider.logger.error("Pipeline de Consumo: No se pudo establecer conexión con la BD en __init__.")

    def close_spider(self, spider):
        """Cierra la conexión a la base de datos."""
        if self.cur:
            self.cur.close()
        if self.connection:
            self.connection.close()
            logging.info("Pipeline de Consumo: Conexión a BD cerrada.")

    def process_item(self, item, spider):
        """Procesa el item (ya limpio por la pipeline anterior) y lo inserta en la tabla de consumo."""
        # Si la conexión falló en __init__, no hacemos nada.
        if not self.cur or not self.connection:
            spider.logger.error(f"Pipeline de Consumo: Omitiendo item por falta de conexión a BD: {item.get('url')}")
            return item # Devolver para no interrumpir el flujo si hay otras pipelines

        adapter = ItemAdapter(item)
        url_item = adapter.get('url')

        # Verificar si la URL ya existe en la tabla de CONSUMO
        try:
            self.cur.execute("SELECT id FROM consumption_analytics WHERE url = %s", (url_item,))
            result = self.cur.fetchone()
            if result:
                spider.logger.info(f"Pipeline de Consumo: Item ya existe en Consumption Zone: {url_item}")
                # Descartamos el item para no procesarlo de nuevo en esta pipeline
                # No usamos DropItem necesariamente, ya que podría haber otras pipelines después.
                # Simplemente lo devolvemos para que siga el flujo, pero no lo insertamos.
                return item
        except psycopg2.Error as e:
            spider.logger.error(f"Pipeline de Consumo: Error al verificar duplicado para {url_item}: {e}")
            # Decidimos devolver el item para no detener todo el proceso por un error de lectura.
            return item

        # Si no existe, procedemos a transformar e insertar
        try:
            # Extraer fuente de la URL
            fuente = self.extract_fuente(url_item)

            # Parsear fecha (el item ya viene con la fecha limpia o None de la pipeline anterior)
            fecha_str_limpia = adapter.get('fecha') # Obtener la fecha (posiblemente ISO o None)
            fecha_noticia_obj = self.parse_fecha(fecha_str_limpia) # Intentar convertir a objeto Date

            # Insertar en la tabla consumption_analytics
            # Nota: refined_id es difícil de obtener aquí de forma fiable si NewspaperCollectorPipeline no lo devuelve. Lo omitiremos o pondremos NULL.
            self.cur.execute("""
                INSERT INTO consumption_analytics
                (titulo, fecha_noticia, seccion, fuente, url, refined_id)
                VALUES (%s, %s, %s, %s, %s, NULL) -- refined_id puesto a NULL
                ON CONFLICT (url) DO NOTHING; -- Seguridad extra contra duplicados
            """, (
                adapter.get('titulo'),
                fecha_noticia_obj, # Objeto Date o None
                adapter.get('seccion'),
                fuente,
                url_item
            ))
            self.connection.commit()
            if self.cur.rowcount > 0:
                 spider.logger.info(f"Pipeline de Consumo: Item insertado en Consumption Zone: {url_item}")

        except psycopg2.Error as e:
            spider.logger.error(f"Pipeline de Consumo: Error al insertar item {url_item}: {e}")
            self.connection.rollback() # Revertir si falla la inserción
        except Exception as e:
            spider.logger.error(f"Pipeline de Consumo: Error inesperado procesando {url_item}: {e}")
            self.connection.rollback()

        # Devolver el item para que siga el flujo (si hubiera más pipelines)
        return item

    # --- Funciones auxiliares (puedes moverlas a un archivo utils si prefieres) ---
    def extract_fuente(self, url):
        """Extrae un nombre de fuente simple del dominio de la URL."""
        if not url:
            return None
        try:
            domain = urlparse(url).netloc
            if 'eldeber.com.bo' in domain:
                return 'eldeber'
            elif 'lostiempos.com' in domain:
                return 'lostiempos'
            elif 'ahoraelpueblo.bo' in domain:
                return 'ahoraelpueblo'
            else:
                parts = domain.split('.')
                if len(parts) >= 2:
                     return parts[-2] if parts[-2] != 'com' else parts[0]
                return domain
        except Exception:
            return None

    def parse_fecha(self, fecha_str):
        """Intenta parsear la fecha string a un objeto date (YYYY-MM-DD)."""
        if not fecha_str:
            return None
        try:
            # Asumimos que fecha_str ya está en ISO 8601 o un formato reconocible por dateutil
            dt_obj = parser.parse(fecha_str)
            return dt_obj.date() # Devuelve solo YYYY-MM-DD
        except (ValueError, TypeError, OverflowError) as e:
            # Loguear el warning sólo si la fecha no era ya None
            if fecha_str is not None:
                 logging.warning(f"Pipeline de Consumo: No se pudo parsear fecha '{fecha_str}': {e}")
            return None # Devuelve None si no se puede parsear