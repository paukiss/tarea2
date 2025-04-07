# Newspaper Collector (Tarea 1)

Este proyecto es una implementación para la **Tarea 1** de Web Scraping. Utiliza **Scrapy** (Python) para extraer noticias de tres periódicos bolivianos: **El Deber**, **Los Tiempos** y **Ahora El Pueblo**.

El sistema recoge noticias recientes, evita duplicados antiguos mediante verificación de fechas, y guarda los resultados en formato **JSON**.

---

## Características Principales

✅ **Scraping de múltiples fuentes dentro del dominio noticias**  
✅ **Extracción estandarizada** de datos clave:  
&nbsp;&nbsp;&nbsp;&nbsp;• Título  
&nbsp;&nbsp;&nbsp;&nbsp;• Descripción  
&nbsp;&nbsp;&nbsp;&nbsp;• Fecha de publicación  
&nbsp;&nbsp;&nbsp;&nbsp;• Sección  
&nbsp;&nbsp;&nbsp;&nbsp;• URL  
✅ **User-Agent personalizado** para evitar bloqueos  
✅ **Paginación automatizada** por fuente  
✅ **Verificación de fechas** para evitar duplicados  
✅ **Salida en formato JSON estructurado**  
✅ **Preparado para ejecución automática cada 2 días**

---

## Estructura de los Datos

Cada noticia extraída se guarda como un objeto JSON con esta estructura:

```json
{
  "data_id": "uuid-v4",
  "titulo": "Título de la noticia",
  "descripcion": "Breve resumen",
  "fecha": "Fecha original (string)",
  "seccion": "Categoría o sección",
  "url": "URL completa del artículo",
  "date_saved": "Fecha y hora del scraping en formato ISO 8601"
}
```

## Ejecutar Programa

1. **Crear entorno virtual:**

```bash
python -m venv venv
```

2. **Activar entorno virtual:**

- En Windows:
  ```bash
  .\\venv\\Scripts\\activate
  ```

- En Linux/macOS:
  ```bash
  source venv/bin/activate
  ```

3. **Instalar dependencias:**

```bash
pip install -r requirements.txt
```

4. **Ejecutar el scraper:**

```bash
python run_schedule.py
```

Este archivo ejecutará el proceso de scraping y generará un archivo `data/salida_{fecha_del_scraping}.json` con las noticias más recientes.
