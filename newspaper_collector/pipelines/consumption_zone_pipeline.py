
from itemadapter import ItemAdapter
from dateutil import parser
from urllib.parse import urlparse
import os
import psycopg2
from dotenv import load_dotenv
import logging


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
            # Nota: refined_id es difícil de obtener aquí de forma fiable si RefinedZonePipeline no lo devuelve. Lo omitiremos o pondremos NULL.
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
       