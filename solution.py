import pandas as pd
from typing import Set
import requests
import sqlite3


def ej_1_cargar_datos_demograficos() -> pd.DataFrame:
  url = "https://public.opendatasoft.com/explore/dataset/us-cities-demographics/download/?format=csv&timezone=Europe/Berlin&lang=en&use_labels_for_header=true&csv_separator=%3B"
  data = pd.read_csv(url, sep=';')
  return data

def ej_2_cargar_calidad_aire(ciudades: Set[str]) -> None:
  lista_datos = []
  for ciudad in ciudades:
    api_url = 'https://api.api-ninjas.com/v1/airquality?city={}'.format(ciudad)
    response = requests.get(api_url, headers={'X-Api-Key': 'YOUR_API_KEY'})
    if response.status_code == requests.codes.ok:
      lista_datos.append(ciudades[ciudad], response.json()['concentration'])
  df_calidad_aire = pd.DataFrame(lista_datos)

def ej_3_limpiar_datos(df):
  # Elimina las columnas 'Race', 'Count' y 'Number of Veterans'
  df = df.drop(['Race', 'Count', 'Number of Veterans'], axis=1)

  # Elimina las filas duplicadas
  df = df.drop_duplicates()



def ej_4_crear_base_datos_y_cargar_tablas(df, nombre_tabla):
    # Paso 1: Crear una conexión a la base de datos SQLite
    conn = sqlite3.connect("lab_ETL.dbp")

    # Paso 2: Guardar los DataFrames en la base de datos
    df.to_sql("nombre_tabla", conn, if_exists="replace", index=False)
    
    # Paso 3: Cerrar la conexión
    conn.close()

df_1 = ej_1_cargar_datos_demograficos()
df_2 = ej_2_cargar_calidad_aire(df_1['City'])
df_1 = ej_3_limpiar_datos(df_1)
df_2 = ej_3_limpiar_datos(df_2)
ej_4_crear_base_datos_y_cargar_tablas(df_1, 'demografia')
ej_4_crear_base_datos_y_cargar_tablas(df_2, 'calidad_aire')
