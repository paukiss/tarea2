
# Newspaper Collector (Tarea 2)

Este proyecto es una implementación para la "Tarea 2", consistente en un web scraper desarrollado con Scrapy (Python) para extraer noticias de tres periódicos bolivianos: El Deber, Los Tiempos y Ahora El Pueblo.

El sistema no solo extrae la información (título, descripción, fecha, sección, URL), sino que también aplica un proceso de limpieza y transformación a los datos antes de almacenarlos de forma dual: en un archivo JSON y en una base de datos PostgreSQL.

## Características Principales

* **Scraping Multi-Fuente:** Extrae datos de:
    * El Deber (`https://eldeber.com.bo/economia`)
    * Los Tiempos (`https://www.lostiempos.com/ultimas-noticias`)
    * Ahora El Pueblo (`https://ahoraelpueblo.bo/index.php/nacional/economia`)
* **Limpieza de Datos:** Implementa un pipeline (`NewspaperCollectorPipeline`) que realiza:
    * Conversión a minúsculas y eliminación de espacios extra.
    * Manejo de valores nulos/vacíos.
    * Limpieza de texto en descripciones (eliminación de emojis, caracteres no alfanuméricos, URLs).
    * Intento de parseo de fechas a formato ISO 8601.
    * Validación de campos esenciales (ej. título no vacío).
* **Almacenamiento Dual:** Guarda los resultados en:
    * Un archivo `news_output.json` (un objeto JSON por línea).
    * Una tabla `newspaper` en una base de datos PostgreSQL (datos limpios y validados).
* **Manejo de Duplicados:** Verifica la existencia de la URL en la base de datos PostgreSQL antes de insertar para evitar duplicados.
* **Estructura Definida:** Utiliza Scrapy Items (`NewspaperItem`) para un manejo estructurado de los datos.
* **Configuración Segura:** Utiliza un archivo `.env` para gestionar las credenciales de la base de datos.
* **Paginación:** Implementa lógica básica para navegar entre páginas en cada sitio web (con límites predefinidos).

## Prerrequisitos

Necesitarás tener instalado lo siguiente en tu sistema:

* **Python:** Versión 3.9 o superior.
* **pip:** El gestor de paquetes de Python (normalmente viene con Python).
* **PostgreSQL:** Un servidor de base de datos PostgreSQL instalado y en ejecución. Deberás poder crear una base de datos y obtener las credenciales de conexión.
* **Git:** (Opcional) Para clonar el repositorio fácilmente.

## Instalación

Sigue estos pasos para configurar el entorno del proyecto:

1.  **Clona el repositorio** (o descarga y descomprime el código fuente):
    ```bash
    git clone <URL-del-repositorio>
    cd <nombre-del-directorio-del-proyecto>
    ```

2.  **(Recomendado) Crea y activa un entorno virtual:**
    ```bash
    python -m venv venv
    ```
    * En Windows:
        ```bash
        .\venv\Scripts\activate
        ```
    * En Linux/macOS:
        ```bash
        source venv/bin/activate
        ```

3.  **Instala las dependencias** de Python desde el archivo `requirements.txt`:
    ```bash
    pip install -r requirements.txt
    ```
    *(Nota: Si no tienes un archivo `requirements.txt`, créalo con el siguiente contenido):*
    ```txt
    # requirements.txt
    Scrapy>=2.6
    psycopg2-binary>=2.9 # Usar psycopg2-binary para facilitar la instalación
    python-dotenv>=0.20
    python-dateutil>=2.8
    emoji>=1.7 # Aunque se use regex, puede ser útil tenerla explícita
    ```

## Configuración

Antes de ejecutar el scraper, necesitas configurar la conexión a tu base de datos PostgreSQL:

1.  **Crea una Base de Datos:** Asegúrate de tener una base de datos creada en tu servidor PostgreSQL. El nombre que elijas debe coincidir con el que pondrás en el archivo de configuración.
2.  **Crea un archivo `.env`:** En la raíz del proyecto (el mismo directorio donde está `scrapy.cfg`), crea un archivo llamado `.env`.
3.  **Añade las variables de entorno:** Copia el siguiente contenido dentro de tu archivo `.env` y reemplaza los valores con tus credenciales reales de PostgreSQL:

    ```dotenv
    # .env - Archivo de configuración de la Base de Datos
    # Reemplaza estos valores con tus datos reales

    DB_HOST=localhost
    DB_USER=postgres
    DB_PASSWORD=lunaluna
    DB_DATABASE=newspaper_data
    DB_PORT=5432
    ```


## Uso

Para ejecutar el scraper:

1.  Asegúrate de que tu servidor PostgreSQL esté **en ejecución**.
2.  Si creaste un entorno virtual, asegúrate de que esté **activado**.
3.  Navega en tu terminal a la **raíz del proyecto** (el directorio que contiene `scrapy.cfg`).
4.  Ejecuta el siguiente comando para lanzar el spider:

    ```bash
    scrapy crawl newspaper_spider
    ```

El spider comenzará a extraer datos, procesarlos a través de los pipelines y almacenarlos. Verás logs de Scrapy en la terminal indicando el progreso y posibles errores.

## Salida del Proceso

Al finalizar la ejecución (o mientras se ejecuta), encontrarás:

1.  **Archivo JSON:** Se creará (o sobrescribirá) un archivo llamado `news_output.json` en la raíz del proyecto. Cada línea de este archivo es un objeto JSON que representa una noticia extraída (posiblemente antes de la limpieza completa).
2.  **Base de Datos PostgreSQL:** Los datos limpios y validados se insertarán en la tabla `newspaper` dentro de la base de datos que configuraste en el archivo `.env`. La tabla será creada automáticamente por el pipeline si no existe. Puedes conectar tu cliente de base de datos (como pgAdmin, DBeaver, o psql) para verificar los datos insertados.

    La estructura de la tabla `newspaper` es aproximadamente:
    ```sql
    CREATE TABLE IF NOT EXISTS newspaper (
        id SERIAL PRIMARY KEY,
        data_id TEXT,
        titulo TEXT,
        descripcion TEXT,
        fecha TEXT,
        seccion TEXT,
        url TEXT UNIQUE, -- Clave para evitar duplicados
        date_saved_iso TEXT -- Fecha de guardado en formato ISO
    );
    ```
