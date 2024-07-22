import pandas as pd
from datetime import datetime, timedelta
import requests
import pytz
from configparser import ConfigParser
from scripts.conexion import conect_to_db, load_to_sql, initialize_tables
import os

def cargar_configuracion(config_file='config/config.ini', section='openweathermap_api_key'):
    parser = ConfigParser()
    try:
        script_dir = os.path.dirname(__file__)
        config_path = os.path.join(script_dir, '..', config_file)
        print(f"Leyendo el archivo de configuración desde: {config_path}")
        parser.read(config_path)
        if parser.has_section(section):
            config = {param[0]: param[1] for param in parser.items(section)}
            return config
        else:
            print(f"Sección {section} no encontrada en el archivo de configuración")
            return None
    except Exception as e:
        print(f"Error al cargar la configuración: {e}")
        return None

def obtener_datos_tiempo(ciudad, pais, api_key):
    utc_now = datetime.utcnow().replace(tzinfo=pytz.utc)  # Fecha y hora en UTC
    fecha_actual = utc_now.astimezone(pytz.timezone('America/Argentina/Buenos_Aires')).strftime('%Y-%m-%d %H:%M:%S')  # Convertir a zona horaria local
    url = f'https://api.openweathermap.org/data/2.5/weather?q={ciudad},{pais}&appid={api_key}'
    respuesta = requests.get(url)   

    if respuesta.status_code == 200:
        datos = respuesta.json()
        clima = datos['weather'][0]['description']
        temperatura_kelvin = datos['main']['temp']
        temperatura_minima_kelvin = datos['main']['temp_min']
        temperatura_maxima_kelvin = datos['main']['temp_max']
        humedad = datos['main']['humidity']
        velocidad_viento = datos['wind']['speed']

        temperatura_celcius = temperatura_kelvin - 273.15
        temperatura_minima_celcius = temperatura_minima_kelvin - 273.15
        temperatura_maxima_celcius = temperatura_maxima_kelvin - 273.15

        df = pd.DataFrame({
            'fecha': [fecha_actual],
            'ciudad': [ciudad],
            'pais': [pais],
            'temperatura': [temperatura_celcius],
            'temperatura_minima': [temperatura_minima_celcius],
            'temperatura_maxima': [temperatura_maxima_celcius],
            'humedad': [humedad],
            'velocidad_viento': [velocidad_viento],
            'condiciones_climaticas': [clima]
        })

        return df
    else:
        print(f'Error en la solicitud para {ciudad}. Códigos de estado:', respuesta.status_code)
        print(f"Mensaje de error de la API: {respuesta.json()['message']}")
        return None

def load_measurements_data(config_file, start, end):
    config = cargar_configuracion(config_file)
    if config is None:
        print("No se pudo cargar la configuración. Abortando.")
        return
    
    api_key = config.get('key')
    if not api_key:
        print("Clave de API no encontrada en la configuración. Abortando.")
        return

    pais = 'AR'
    capitales = ['Buenos Aires', 'Cordoba', 'Rosario', 'Misiones']

    clima_dfs = []
    for ciudad in capitales:
        data_ciudad = obtener_datos_tiempo(ciudad, pais, api_key)
        if data_ciudad is not None:
            clima_dfs.append(data_ciudad)
    
    if clima_dfs:
        clima_df = pd.concat(clima_dfs)
        engine = conect_to_db(config_file, 'postgresql')
        if engine:
            initialize_tables(engine, 'weather_data')
            load_to_sql(clima_df, 'weather_data', engine, 'fecha')

def main():
    start = datetime.utcnow() - timedelta(hours=1)
    end = start + timedelta(hours=1)
    end = start.strftime("%Y-%m-%dT%H:59:59Z")
    start = start.strftime("%Y-%m-%dT%H:00:00Z")
    load_measurements_data("config/config.ini", start, end)

if __name__ == "__main__":
    main()
