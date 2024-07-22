from datetime import datetime, timedelta

import pandas as pd

from conexion import *

base_url = "https://api.luchtmeetnet.nl/open_api"


def load_stations_data(config_file):
    """
    Proceso ETL para los datos de estaciones
    """
    base_url = "https://api.luchtmeetnet.nl/open_api"
    df_stations_tmp = None

    try:
        # Obtener datos iniciales de estaciones
        df_stations_tmp = traer_data(base_url, "stations", params={"organization_id": 1})
        if df_stations_tmp is None:
            raise Exception("Failed to retrieve initial station data.")

        stations = []
        for station in df_stations_tmp['number'].unique():
            try:
                df_station = traer_data(base_url, f"stations/{station}")
                if df_station is not None:
                    df_station["station"] = station
                    stations.append(df_station)
            except requests.exceptions.HTTPError as http_err:
                if http_err.response.status_code == 429:
                    logging.error(f"HTTP Error 429: Too Many Requests for station {station}")
                    raise http_err
                else:
                    logging.error(f"HTTP Error for station {station}: {http_err}")
                    continue

        df_stations = pd.concat(stations)
        df_stations = df_stations[['station', 'type', 'municipality', 'url', 'province', 'organisation', 'location', 'year_start']]

        # Conectar a la base de datos
        engine = conect_to_db(config_file, "postgresql")
        if engine is None:
            raise Exception("Failed to connect to the database.")

        # Cargar datos en la base de datos
        load_to_sql(df_stations, "stations", engine, check_field="station")

    except Exception as e:
        logging.error(f"Error al procesar los datos de estaciones: {e}")
        raise e

def load_measurements_data(config_file, start, end):
    """
    Proceso ETL para los datos de mediciones
    """
    try:
        df_measurements = traer_data(
            base_url, "measurements",
            params={"start": start, "end": end}
            )
        
        engine = conect_to_db(config_file, "postgresql")
        load_to_sql(df_measurements, "measurements", engine, check_field="station_number")
    except Exception as e:
        logging.error(f"Error al obtener datos de {base_url}: {e}")
        raise e

if __name__ == "__main__":
    config_file = "config/config.ini"
    load_stations_data(config_file)

    start = datetime.utcnow() - timedelta(hours=1)
    end = start.strftime("%Y-%m-%dT%H:59:59Z")
    start = start.strftime("%Y-%m-%dT%H:00:00Z")
    load_measurements_data("config/config.ini", start, end)