# dashboard/viz.py

import streamlit as st
import pandas as pd
import requests
import os
from dotenv import load_dotenv
from datetime import datetime, date # Import 'date'
import logging
# Ya no necesitamos importar psycopg2 directamente si usamos st.connection

# Configurar logging básico
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Configuración de la Página Streamlit ---
st.set_page_config(
    page_title="Dashboard de Noticias",
    page_icon="📰",
    layout="wide"
)

# --- Cargar Clave API Externa desde .env ---
# Nota: Cargamos desde .env en la raíz del proyecto, NO desde secrets.toml
dotenv_path = os.path.join(os.path.dirname(__file__), '.env') # Ruta al .env en la raíz
load_dotenv(dotenv_path=dotenv_path)
OWM_API_KEY = os.getenv('OPENWEATHERMAP_API_KEY')

# --- Conexión a la Base de Datos (Usando st.connection y secrets.toml) ---
# st.connection maneja la creación y cacheo de la conexión automáticamente
# Se conecta usando la configuración [connections.postgresql] de secrets.toml
@st.cache_resource # Aunque st.connection cachea, podemos añadir por si acaso
def get_connection():
     logging.info("Inicializando conexión a través de st.connection...")
     try:
         conn = st.connection("postgresql", type="sql")
         logging.info("Conexión via st.connection exitosa.")
         return conn
     except Exception as e:
         logging.error(f"Error al inicializar st.connection: {e}")
         st.error(f"Error al conectar a la base de datos via st.connection: {e}")
         return None

# --- Carga de Datos (Cacheada) ---
@st.cache_data(ttl=600) # Cachear por 10 minutos
def load_data(_conn):
    """Carga datos de la tabla consumption_analytics usando st.connection."""
    if _conn is None:
        return pd.DataFrame()
    logging.info("Cargando datos desde Consumption Zone via st.connection...")
    try:
        # Usar el método .query() de st.connection
        query = "SELECT titulo, fecha_noticia, seccion, fuente, url FROM consumption_analytics WHERE fecha_noticia IS NOT NULL ORDER BY fecha_noticia DESC;"
        df = _conn.query(query, ttl=600) # Podemos añadir ttl aquí también

        # st.connection a veces devuelve tipos específicos, aseguramos formato
        df['fecha_noticia'] = pd.to_datetime(df['fecha_noticia'], errors='coerce')
        df.dropna(subset=['fecha_noticia'], inplace=True) # Eliminar filas si la conversión falló

        logging.info(f"Datos cargados via st.connection: {len(df)} filas.")
        return df
    except Exception as e:
        logging.error(f"Error al cargar datos via st.connection: {e}")
        st.error(f"Error al cargar datos de la base de datos: {e}")
        return pd.DataFrame()

# --- Integración API Externa (Ejemplo: OpenWeatherMap - Sin cambios) ---
@st.cache_data(ttl=600) # Cachear datos de API por 10 minutos
def get_weather_data(api_key, city="La Paz,BO"):
    """Obtiene datos del clima de OpenWeatherMap."""
    if not api_key:
        st.warning("API Key de OpenWeatherMap no configurada en .env")
        return None
    base_url = "http://api.openweathermap.org/data/2.5/weather?"
    complete_url = base_url + "appid=" + api_key + "&q=" + city + "&units=metric&lang=es"
    logging.info(f"Consultando API del clima para {city}...")
    try:
        response = requests.get(complete_url)
        response.raise_for_status()
        data = response.json()
        if data.get("cod") != 200:
             logging.error(f"Error API Clima: {data.get('message', 'Respuesta inválida')}")
             st.warning(f"No se pudo obtener el clima: {data.get('message', 'Respuesta inválida')}")
             return None
        logging.info("Datos del clima obtenidos.")
        return data
    except requests.exceptions.RequestException as e:
        logging.error(f"Error al conectar con API del clima: {e}")
        st.error(f"Error al conectar con la API del clima: {e}")
        return None
    except Exception as e:
        logging.error(f"Error inesperado con API del clima: {e}")
        st.error(f"Error inesperado al obtener el clima: {e}")
        return None

# --- Construcción del Dashboard ---
st.title("📰 Dashboard de Recolección de Noticias")

# Obtener conexión y cargar datos
conn = get_connection() # Llama a la función que usa st.connection
if conn:
     df_noticias = load_data(conn)
else:
     df_noticias = pd.DataFrame() # DataFrame vacío si no hay conexión

if df_noticias.empty and conn is not None:
    st.warning("No se encontraron datos de noticias en la base de datos (Consumption Zone). ¿Se ha ejecutado el proceso ETL?")
    weather_data = get_weather_data(OWM_API_KEY) # Intentar cargar clima de todos modos
elif df_noticias.empty and conn is None:
     # El error ya se mostró en get_connection()
     st.stop() # Detener ejecución si no hay DB
else:
     # --- Sección API Externa (Clima) ---
     st.subheader("☀️ Clima Actual (La Paz)")
     weather_data = get_weather_data(OWM_API_KEY)
     if weather_data and weather_data.get("main"):
         col1, col2, col3 = st.columns(3)
         col1.metric("Temperatura", f"{weather_data['main']['temp']} °C")
         col2.metric("Sensación Térmica", f"{weather_data['main']['feels_like']} °C")
         weather_desc = weather_data.get('weather', [{}])[0].get('description', 'N/A')
         col3.metric("Descripción", weather_desc.capitalize())
     else:
         st.info("No se pudieron obtener los datos del clima actual.")

     st.divider()

     # --- Filtros en la Barra Lateral ---
     st.sidebar.header("Filtros")
     # Filtro por Fuente
     # Asegurarse que 'fuente' existe y no tiene nulos antes de usar .unique()
     if 'fuente' in df_noticias.columns and df_noticias['fuente'].notna().any():
         fuentes_disponibles = sorted(df_noticias['fuente'].dropna().unique().tolist())
         fuentes_seleccionadas = st.sidebar.multiselect(
             "Selecciona Fuente(s):",
             options=fuentes_disponibles,
             default=fuentes_disponibles
         )
     else:
         st.sidebar.warning("No hay datos de 'fuente' disponibles para filtrar.")
         fuentes_seleccionadas = [] # Lista vacía si no hay fuentes

     # Filtro por Rango de Fechas
     # Asegurarse que 'fecha_noticia' existe y no tiene nulos antes de min/max
     if 'fecha_noticia' in df_noticias.columns and df_noticias['fecha_noticia'].notna().any():
         fecha_min = df_noticias['fecha_noticia'].min().date()
         fecha_max = df_noticias['fecha_noticia'].max().date()
         # Verificar que min <= max antes de crear los widgets
         if fecha_min <= fecha_max:
             fecha_inicio = st.sidebar.date_input(
                 "Fecha Inicio:",
                 value=fecha_min,
                 min_value=fecha_min,
                 max_value=fecha_max
             )
             fecha_fin = st.sidebar.date_input(
                 "Fecha Fin:",
                 value=fecha_max,
                 min_value=fecha_min,
                 max_value=fecha_max
             )
             if fecha_inicio > fecha_fin:
                 st.sidebar.error("Error: La fecha de inicio debe ser anterior o igual a la fecha de fin.")
                 st.stop()
         else:
            st.sidebar.warning("Rango de fechas inválido en los datos.")
            # Establecer fechas por defecto si el rango es inválido
            fecha_inicio = date.today() - timedelta(days=30)
            fecha_fin = date.today()
     else:
         st.sidebar.warning("No hay datos de 'fecha_noticia' disponibles para filtrar.")
         fecha_inicio = date.today() - timedelta(days=30) # Defaults si no hay fechas
         fecha_fin = date.today()

     # --- Aplicar Filtros ---
     # Construir condiciones de filtro dinámicamente
     condiciones = []
     if fuentes_seleccionadas and 'fuente' in df_noticias.columns:
         condiciones.append(df_noticias['fuente'].isin(fuentes_seleccionadas))
     if fecha_inicio and fecha_fin and 'fecha_noticia' in df_noticias.columns:
        # Asegurarse que fecha_noticia sea comparable con date
        condiciones.append(df_noticias['fecha_noticia'].dt.date >= fecha_inicio)
        condiciones.append(df_noticias['fecha_noticia'].dt.date <= fecha_fin)

     # Aplicar filtro combinado si hay condiciones
     if condiciones:
         df_filtrado = df_noticias[pd.concat(condiciones, axis=1).all(axis=1)]
     else:
         df_filtrado = df_noticias.copy() # Sin filtros aplicados o posibles

     # --- Métricas Principales ---
     st.subheader("Estadísticas Generales (Filtradas)")
     total_noticias_filtradas = len(df_filtrado)
     st.metric("Total Noticias Encontradas", f"{total_noticias_filtradas}")

     # --- Visualizaciones ---
     st.subheader("Visualización de Datos (Filtrados)")
     if total_noticias_filtradas > 0:
         # 1. Noticias por Día
         st.write("📈 Noticias por Día")
         # Agrupar por fecha (dt.date) y contar URLs únicas o títulos
         noticias_por_dia = df_filtrado.groupby(df_filtrado['fecha_noticia'].dt.date)['url'].nunique()
         if not noticias_por_dia.empty:
             st.line_chart(noticias_por_dia)
         else:
             st.info("No hay datos para graficar noticias por día con los filtros actuales.")

         # 2. Noticias por Fuente
         st.write("📊 Noticias por Fuente")
         if 'fuente' in df_filtrado.columns:
             noticias_por_fuente = df_filtrado['fuente'].value_counts()
             if not noticias_por_fuente.empty:
                 st.bar_chart(noticias_por_fuente)
             else:
                  st.info("No hay datos de fuente con los filtros actuales.")
         else:
              st.info("Columna 'fuente' no encontrada.")


         # 3. Noticias por Sección (Top 10)
         st.write("📊 Top 10 Secciones")
         if 'seccion' in df_filtrado.columns:
             noticias_por_seccion = df_filtrado['seccion'].value_counts().nlargest(10)
             if not noticias_por_seccion.empty:
                 st.bar_chart(noticias_por_seccion)
             else:
                  st.info("No hay datos de sección con los filtros actuales.")
         else:
             st.info("Columna 'seccion' no encontrada.")


         # 4. Tabla de Datos
         st.subheader("Muestra de Datos Filtrados")
         st.dataframe(
            df_filtrado[['titulo', 'fecha_noticia', 'seccion', 'fuente', 'url']].head(20),
            use_container_width=True # Ajustar al ancho del contenedor
         )
     else:
         st.info("No hay noticias para mostrar con los filtros seleccionados.")