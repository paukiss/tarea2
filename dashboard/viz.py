# dashboard/viz.py

import streamlit as st
import pandas as pd
import requests
import os
from dotenv import load_dotenv
from datetime import datetime, date, time, timedelta # Asegurar imports
import logging

# Configurar logging básico
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Configuración de la Página Streamlit ---
st.set_page_config(
    page_title="Dashboard de Noticias",
    page_icon="📰",
    layout="wide"
)

# --- Cargar Clave API Externa desde .env ---
# (Ajusta la ruta a tu .env si es necesario)
dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
if not os.path.exists(dotenv_path):
     dotenv_path = os.path.join(os.path.dirname(__file__), '..', '..', '.env')
load_dotenv(dotenv_path=dotenv_path)
OWM_API_KEY = os.getenv('OPENWEATHERMAP_API_KEY')
if not OWM_API_KEY:
     logging.warning("Variable OPENWEATHERMAP_API_KEY no encontrada en .env")


# --- Conexión a la Base de Datos (Usando st.connection) ---
@st.cache_resource
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

# --- Carga y Procesamiento de Datos (Manteniendo Columnas Separadas) ---
@st.cache_data(ttl=600)
def load_data(_conn):
    """Carga datos de consumption_analytics, asegurando tipos correctos para fecha y hora."""
    if _conn is None:
        return pd.DataFrame()
    logging.info("Cargando datos (fecha y hora separadas) desde Consumption Zone via st.connection...")
    try:
        # ----- Query SQL (sin cambios respecto a la anterior) -----
        query = """
            SELECT titulo, fecha_noticia, hora_noticia, seccion, fuente, url
            FROM consumption_analytics
            WHERE fecha_noticia IS NOT NULL AND hora_noticia IS NOT NULL
            ORDER BY fecha_noticia DESC, hora_noticia DESC;
        """
        df = _conn.query(query, ttl=600)
        logging.info(f"Datos crudos cargados: {len(df)} filas.")

        if df.empty:
             logging.info("No se encontraron datos en la tabla consumption_analytics.")
             return df

        # ----- MODIFICACIÓN: Asegurar tipos correctos (SIN COMBINAR) -----
        # Convertir fecha_noticia a objetos date
        # pd.to_datetime primero, luego .dt.date para asegurar solo la fecha
        df['fecha_noticia'] = pd.to_datetime(df['fecha_noticia'], errors='coerce').dt.date

        # Convertir hora_noticia a objetos time
        def safe_to_time(t):
            if pd.isna(t): return None
            if isinstance(t, time): return t
            try: return pd.to_datetime(str(t), format='%H:%M:%S.%f').time()
            except (ValueError, TypeError):
                try: return pd.to_datetime(str(t), format='%H:%M:%S').time()
                except (ValueError, TypeError):
                     logging.warning(f"No se pudo convertir '{t}' a objeto time.")
                     return None
        df['hora_noticia'] = df['hora_noticia'].apply(safe_to_time)

        # Eliminar filas donde la conversión falló o los originales eran nulos
        original_len = len(df)
        df.dropna(subset=['fecha_noticia', 'hora_noticia'], inplace=True)
        if len(df) < original_len:
             logging.warning(f"Se eliminaron {original_len - len(df)} filas por fecha u hora inválida/nula.")
        # -------------------------------------------------------------

        logging.info(f"Datos procesados con fecha y hora separadas: {len(df)} filas.")
        return df
    except Exception as e:
        logging.error(f"Error al cargar/procesar datos via st.connection: {e}", exc_info=True)
        st.error(f"Error al cargar datos de la base de datos: {e}")
        return pd.DataFrame()

# --- Integración API Externa (Sin cambios) ---
@st.cache_data(ttl=600)
def get_weather_data(api_key, city="La Paz,BO"):
    # ... (código idéntico) ...
    if not api_key:
        logging.warning("API Key de OpenWeatherMap no configurada.")
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
conn = get_connection()
if conn:
     df_noticias = load_data(conn)
else:
     df_noticias = pd.DataFrame()

if df_noticias.empty and conn is not None:
    st.warning("No se encontraron datos válidos de noticias en la base de datos (Consumption Zone). Verifique el proceso ETL y los datos en la tabla.")
elif df_noticias.empty and conn is None:
     st.info("Esperando conexión a la base de datos...")
else:
     st.success(f"Datos cargados ({len(df_noticias)} noticias).")

# --- Sección API Externa (Clima) ---
# ... (código sin cambios) ...
st.subheader("☀️ Clima Actual (La Paz)")
weather_data = get_weather_data(OWM_API_KEY)
if weather_data and weather_data.get("main"):
    col1, col2, col3 = st.columns(3)
    col1.metric("Temperatura", f"{weather_data['main']['temp']} °C")
    col2.metric("Sensación Térmica", f"{weather_data['main']['feels_like']} °C")
    weather_desc = weather_data.get('weather', [{}])[0].get('description', 'N/A')
    col3.metric("Descripción", weather_desc.capitalize())
else:
    st.info("No se pudieron obtener los datos del clima actual (verificar API key).")
st.divider()


# --- Filtros en la Barra Lateral ---
st.sidebar.header("Filtros")

# Filtro por Fuente (sin cambios)
if not df_noticias.empty and 'fuente' in df_noticias.columns and df_noticias['fuente'].notna().any():
    fuentes_disponibles = sorted(df_noticias['fuente'].dropna().unique().tolist())
    fuentes_seleccionadas = st.sidebar.multiselect(
        "Selecciona Fuente(s):", options=fuentes_disponibles, default=fuentes_disponibles
    )
elif not df_noticias.empty:
     st.sidebar.warning("Columna 'fuente' no disponible para filtrar.")
     fuentes_seleccionadas = []
else:
     fuentes_seleccionadas = []

# Filtro por Rango de Fechas
# ----- MODIFICACIÓN: Usar 'fecha_noticia' directamente -----
fecha_inicio = None
fecha_fin = None
if not df_noticias.empty and 'fecha_noticia' in df_noticias.columns and df_noticias['fecha_noticia'].notna().any():
    try:
        # La columna 'fecha_noticia' ya contiene objetos date (o debería)
        fecha_min = df_noticias['fecha_noticia'].min()
        fecha_max = df_noticias['fecha_noticia'].max()

        # Verificar que sean objetos date válidos
        if isinstance(fecha_min, date) and isinstance(fecha_max, date):
            if fecha_min <= fecha_max:
                fecha_inicio = st.sidebar.date_input(
                    "Fecha Inicio:", value=fecha_min, min_value=fecha_min, max_value=fecha_max
                )
                fecha_fin = st.sidebar.date_input(
                    "Fecha Fin:", value=fecha_max, min_value=fecha_min, max_value=fecha_max
                )
                if fecha_inicio and fecha_fin and fecha_inicio > fecha_fin:
                    st.sidebar.error("Error: La fecha de inicio debe ser anterior o igual a la fecha de fin.")
                    fecha_inicio = None
                    fecha_fin = None
            else:
                st.sidebar.warning("Rango de fechas inválido detectado.")
        else:
             st.sidebar.warning("Tipos de fecha inválidos encontrados en la columna 'fecha_noticia'.")

    except Exception as e:
         st.sidebar.error(f"Error al procesar rango de fechas: {e}")

# Usar defaults si algo falló
if fecha_inicio is None or fecha_fin is None:
     if not df_noticias.empty:
          st.sidebar.warning("No se pudo determinar el rango de fechas. Usando últimos 30 días.")
     today = date.today()
     fecha_fin_default = today
     fecha_inicio_default = today - timedelta(days=30)
     # Mostrar los date_input con defaults para que el usuario pueda cambiar
     fecha_inicio = st.sidebar.date_input("Fecha Inicio:", value=fecha_inicio_default)
     fecha_fin = st.sidebar.date_input("Fecha Fin:", value=fecha_fin_default)
     if fecha_inicio and fecha_fin and fecha_inicio > fecha_fin:
         st.sidebar.error("Fecha de inicio inválida.")
         fecha_inicio = None
         fecha_fin = None
# --------------------------------------------------------------

# --- Aplicar Filtros ---
df_filtrado = df_noticias.copy()

if not df_filtrado.empty:
     if fuentes_seleccionadas:
         df_filtrado = df_filtrado[df_filtrado['fuente'].isin(fuentes_seleccionadas)]

     if fecha_inicio and fecha_fin and 'fecha_noticia' in df_filtrado.columns:
         try:
             # Comparar directamente con la columna de fecha
             df_filtrado = df_filtrado[
                 (df_filtrado['fecha_noticia'] >= fecha_inicio) &
                 (df_filtrado['fecha_noticia'] <= fecha_fin)
             ]
         except TypeError as e:
             st.error(f"Error al aplicar filtro de fecha: {e}. Verifique tipos.")


# --- Métricas Principales ---
# ... (sin cambios) ...
st.subheader("Estadísticas Generales (Filtradas)")
total_noticias_filtradas = len(df_filtrado)
st.metric("Total Noticias Encontradas", f"{total_noticias_filtradas}")

# --- Visualizaciones ---
st.subheader("Visualización de Datos (Filtrados)")

if df_filtrado.empty:
    st.info("No hay noticias para mostrar con los filtros seleccionados.")
else:
    # 1. Noticias por Día
    st.write("📈 Noticias por Día")
    # ----- MODIFICACIÓN: Agrupar por 'fecha_noticia' -----
    if 'fecha_noticia' in df_filtrado.columns:
        noticias_por_dia = df_filtrado.groupby('fecha_noticia')['url'].nunique()
        if not noticias_por_dia.empty:
            # Asegurarse que el índice sea tipo fecha para el gráfico
            noticias_por_dia.index = pd.to_datetime(noticias_por_dia.index)
            st.line_chart(noticias_por_dia)
        else:
            st.info("No hay datos para graficar noticias por día con los filtros actuales.")
    else:
        st.info("Columna 'fecha_noticia' no encontrada para graficar por día.")
    # ----------------------------------------------------

    # 2. Noticias por Fuente (sin cambios)
    # ...
    st.write("📊 Noticias por Fuente")
    if 'fuente' in df_filtrado.columns:
        noticias_por_fuente = df_filtrado['fuente'].value_counts()
        if not noticias_por_fuente.empty:
            st.bar_chart(noticias_por_fuente)
        else:
             st.info("No hay datos de fuente con los filtros actuales.")
    else:
         st.info("Columna 'fuente' no encontrada.")

    # 3. Noticias por Sección (Top 10) (sin cambios)
    # ...
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
    # ----- MODIFICACIÓN: Mostrar 'fecha_noticia' y 'hora_noticia' -----
    columnas_a_mostrar = ['titulo', 'fecha_noticia', 'hora_noticia', 'seccion', 'fuente', 'url']
    # Verificar que las columnas existan antes de intentar mostrarlas
    columnas_disponibles = [col for col in columnas_a_mostrar if col in df_filtrado.columns]

    if 'fecha_noticia' in columnas_disponibles and 'hora_noticia' in columnas_disponibles:
         st.dataframe(
            df_filtrado[columnas_disponibles].head(20),
            column_config={
                "fecha_noticia": st.column_config.DateColumn(
                    "Fecha Noticia",
                    format="YYYY-MM-DD", # Formato deseado para fecha
                ),
                "hora_noticia": st.column_config.TimeColumn(
                    "Hora Noticia",
                    format="HH:mm:ss", # Formato deseado para hora
                )
            },
            use_container_width=True
         )
    else:
         st.warning("Columnas de fecha y/o hora no encontradas. Mostrando tabla parcial.")
         # Mostrar tabla sin columnas de fecha/hora si faltan
         fallback_columns = [col for col in columnas_disponibles if col not in ['fecha_noticia', 'hora_noticia']]
         if fallback_columns:
              st.dataframe(df_filtrado[fallback_columns].head(20), use_container_width=True)
         else:
              st.info("No hay columnas de datos básicos para mostrar.")

    # -----------------------------------------------------------------