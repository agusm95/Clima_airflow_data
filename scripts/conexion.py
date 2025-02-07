import logging
import json
from configparser import ConfigParser

import requests
from sqlalchemy import create_engine
from pandas import json_normalize, to_datetime, concat
from sqlalchemy import text

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def traer_data(base_url, endpoint, params=None):
    """
    Realiza una solicitud GET a una API para obtener datos.

    Parametros:
    base_url: La URL base de la API.
    endpoint: El endpoint (ruta) de la API para obtener datos específicos.
    params: Los parámetros de la solicitud GET.
    
    Retorno:
    Un DataFrame con los datos obtenidos de la API.
    """
    try:
        endpoint_url= f"{base_url}/{endpoint}"
        logging.info(f"Obteniendo datos de {endpoint_url}...")
        logging.info(f"Parametros: {params}")
        response = requests.get(endpoint_url,params=params)
        response.raise_for_status()   # Levanta una excepción si hay un error en la respuesta HTTP.
        logging.info(response.url)
        logging.info("Datos obtenios exitosamente... Procesando datos...")
        data = response.json()
        data = data["data"]
        df = json_normalize(data)

        logging.info(f"Datos obtenios exitosamente")

    except requests.exceptions.RequestException as e:
         # Capturar cualquier error de solicitud, como errores de HTTP
         logging.error(f"La peticion a{base_url} ha fallado{e}")
         return None
    except Exception as e:
        # Registrar cualquier otro error
        logging.exception(f"Error al obtener datos de {base_url}: {e}")
    
    return df

def conect_to_db(config_file, section):
    """
    Crea una conexión a la base de datos especificada en el archivo de configuración.

    Parameters:
    config_file (str): La ruta del archivo de configuración.
    section (str): La sección del archivo de configuración que contiene los datos de la base de datos.

    Returns:
    sqlalchemy.engine.base.Engine: Un objeto de conexión a la base de datos.
    """
    try:
        parser = ConfigParser()
        parser.read(config_file)

        db = {}
        if parser.has_section(section):
            params = parser.items(section)
            db = {param[0]: param[1] for param in params}

            logging.info("Conectándose a la base de datos...")
            engine = create_engine(
                f"postgresql://{db['user']}:{db['password']}@{db['host']}:{db['port']}/{db['dbname']}"
                , connect_args={"options": f"-c search_path={db['schema']}"}
                )

            logging.info("Conexión a la base de datos establecida exitosamente")
            return engine

        else:
            logging.error(f"Sección {section} no encontrada en el archivo de configuración")
            return None
        
    except Exception as e:
        logging.error(f"Error al conectarse a la base de datos: {e}")
        return None

from sqlalchemy import text

def initialize_tables(engine, table_name):
    try:
        with engine.connect() as conn:
            logger.info(f"Verificando y creando tablas {table_name} y {table_name}_stg si no existen...")
            
            # Verificar si la tabla principal ya existe
            table_exists = conn.execute(text(f"""
                SELECT EXISTS (
                    SELECT 1
                    FROM information_schema.tables
                    WHERE  table_name = '{table_name}'
                );
            """)).scalar()

            if not table_exists:
                conn.execute(text(f"""
                    CREATE TABLE {table_name} (
                        fecha TIMESTAMP,
                        ciudad VARCHAR(50),
                        pais VARCHAR(50),
                        temperatura FLOAT,
                        temperatura_minima FLOAT,
                        temperatura_maxima FLOAT,
                        humedad INT,
                        velocidad_viento FLOAT,
                        condiciones_climaticas VARCHAR(50),
                        PRIMARY KEY (fecha, ciudad)
                    );
                """))
                logger.info(f"Tabla {table_name} creada exitosamente")
            else:
                logger.info(f"La tabla {table_name} ya existe")

            # Verificar si la tabla _stg ya existe
            table_exists_stg = conn.execute(text(f"""
                SELECT EXISTS (
                    SELECT 1
                    FROM information_schema.tables
                    WHERE table_name = '{table_name}_stg'
                );
            """)).scalar()

            if not table_exists_stg:
                conn.execute(text(f"""
                    CREATE TABLE {table_name}_stg (
                        fecha TIMESTAMP,
                        ciudad VARCHAR(50),
                        pais VARCHAR(50),
                        temperatura FLOAT,
                        temperatura_minima FLOAT,
                        temperatura_maxima FLOAT,
                        humedad INT,
                        velocidad_viento FLOAT,
                        condiciones_climaticas VARCHAR(50)
                    );
                """))
                logger.info(f"Tabla {table_name}_stg creada exitosamente")
            else:
                logger.info(f"La tabla {table_name}_stg ya existe")

    except Exception as e:
        logger.error(f"Error al inicializar las tablas: {e}")
        raise


def load_to_sql(df, table_name, engine, check_field):
    """
    Cargar un dataframe en una tabla de base de datos,
    usando una tabla intermedia o stage para control de duplicados.

    Parameters:
    df (pandas.DataFrame): El DataFrame a cargar en la base de datos.
    table_name (str): El nombre de la tabla en la base de datos.
    engine (sqlalchemy.engine.base.Engine): Un objeto de conexión a la base de datos.
    check_field (str): El nombre de la columna que se usará para controlar duplicados.
    """
    try:
        with engine.connect() as conn:
            logger.info(f"Cargando datos en la tabla {table_name}_stg...")
            conn.execute(f"TRUNCATE TABLE {table_name}_stg")
            df.to_sql(f"{table_name}_stg", conn, if_exists="append", index=False)
            logger.info("Datos cargados exitosamente en la tabla intermedia")

            logger.info(f"Cargando datos en la tabla {table_name}...")
            conn.execute(f"""
                BEGIN;
                DELETE FROM {table_name}
                USING {table_name}_stg
                WHERE {table_name}.{check_field} = {table_name}_stg.{check_field};
                INSERT INTO {table_name} SELECT * FROM {table_name}_stg;
                COMMIT;
            """)
            logger.info("Datos cargados exitosamente en la tabla final")

    except Exception as e:
        logger.error(f"Error al cargar los datos en la base de datos: {e}")
        raise
