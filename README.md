# Newspaper Collector (Tarea 2)

Este proyecto es una implementación de la **Tarea 2: Limpieza y Transformación de Datos con Scrapy**, cuyo objetivo es extraer, limpiar y almacenar artículos de noticias desde tres periódicos bolivianos: **El Deber**, **Los Tiempos** y **Ahora El Pueblo**.

El sistema aplica un flujo completo de extracción, validación, transformación y almacenamiento dual, en un archivo **JSON** y en una base de datos **PostgreSQL**.

---

## Características Principales

🔍 **Scraping Multi-Fuente:**  
El sistema extrae noticias desde:
- [El Deber](https://eldeber.com.bo/economia)
- [Los Tiempos](https://www.lostiempos.com/actualidad/economia)
- [Ahora El Pueblo](https://ahoraelpueblo.bo/index.php/nacional/economia)

🧹 **Limpieza y Validación de Datos:**  
Se realiza mediante `pipelines.py` e incluye:
- Eliminación de espacios en blanco, caracteres especiales, emojis y URLs en las descripciones.
- Conversión de fechas al formato ISO 8601.
- Validación obligatoria de campos clave como `titulo`, `fecha`, `url`.
- Conversión de campos a minúsculas si es necesario.

🧱 **Estructura Definida con Scrapy Items:**  
Se utilizó `items.py` para definir la estructura esperada de los datos (`NewspaperItem`), incluyendo validaciones de tipo por campo.

🗃️ **Almacenamiento Dual:**
- Se genera un archivo JSON (`news_output.json`) con los datos limpios.
- Se insertan los datos en una base de datos PostgreSQL, usando `psycopg2`.

🔁 **Prevención de Duplicados:**  
Antes de insertar, se verifica la existencia previa de la URL en la base de datos para evitar duplicados.

📄 **Paginación:**  
Cada spider incluye lógica para recorrer múltiples páginas sin repetir contenido.

---

## Flujo de Trabajo

1. **Inicio del spider (`newspaper_spider`)**
2. **Extracción de datos desde los 3 periódicos**
3. **Limpieza y validación en el pipeline**
4. **Almacenamiento en JSON y PostgreSQL**

---

## Requisitos

- **Python 3.9+**
- **PostgreSQL** (instalado y corriendo)
- **pip**

---

## Instalación

1. Crear entorno virtual (opcional pero recomendado):

```bash
python -m venv venv
```

- Windows: `.\venv\Scripts\activate`  
- Linux/macOS: `source venv/bin/activate`

2. Instalar dependencias:

```bash
pip install -r requirements.txt
```

## Configuración

1. Crear un archivo `.env` en la raíz del proyecto:

```dotenv
DB_HOST=localhost
DB_USER=postgres
DB_PASSWORD=lunaluna
DB_DATABASE=newspaper_data
DB_PORT=5432
```

2. Asegúrate de tener creada la base de datos `newspaper_data` en PostgreSQL.

---

## Ejecución

Con tu entorno virtual activo y PostgreSQL corriendo:

```bash
python run_schedule.py
```

El scraper comenzará el proceso completo de scraping, limpieza y almacenamiento.


