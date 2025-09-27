# etl_utils.py

# --- Importación de librerías ---
import json
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from astrapy.db import AstraDB
from pyspark.sql import SparkSession

# --- Cargar variables de entorno desde .env ---
import os
from dotenv import load_dotenv

# --- Conexión a Astra DB ---
from astrapy import DataAPIClient


def connect_to_astra():
    """
    Crea y devuelve una conexión a Astra DB utilizando credenciales almacenadas en un archivo .env.

    Basado en el snippet oficial de DataStax Astra.

    Requisitos:
    - Archivo .env en la raíz del proyecto con:
        ASTRA_DB_TOKEN=tu_token_aquí
    """
    # Cargar variables de entorno
    load_dotenv()

    # Obtener token desde .env
    token_astra = os.getenv("ASTRA_DB_TOKEN")
    if not token_astra:
        raise ValueError("No se encontró ASTRA_DB_TOKEN en el archivo .env")

    # Inicializar cliente Astra
    client = DataAPIClient(token_astra)
    db = client.get_database_by_api_endpoint(
        "https://35e119e7-5c60-46cd-9184-5472ad94217b-us-east-2.apps.astra.datastax.com",
        keyspace="data_users",
    )

    return db


# --- Funciones ETL auxiliares ---
def insertar_si_vacia(df: pd.DataFrame, coleccion: str, db: AstraDB) -> None:
    """
    Inserta los datos de un DataFrame en una colección de Astra DB solo si la colección está vacía.

    Parámetros
    ----------
    df : pd.DataFrame
        DataFrame con los datos a insertar.
    coleccion : str
        Nombre de la colección en la base de datos Astra.
    db : astrapy.db.AstraDB
        Objeto de conexión a la base de datos Astra.

    Comportamiento
    --------------
    - Reemplaza NaN por None para compatibilidad JSON.
    - Convierte objetos datetime a 'YYYY-MM-DD' si corresponde.
    - Crea la colección si no existe.
    - Inserta únicamente si la colección no tiene documentos.
    """
    # Reemplaza NaN por None
    df_clean = df.where(pd.notnull(df), None)

    # Convierte fechas a string (si el valor tiene .strftime)
    df_clean = df_clean.applymap(
        lambda x: x.strftime('%Y-%m-%d') if hasattr(x, 'strftime') else x
    )

    # Convierte a lista de dicts JSON-compatibles
    records = (
        df_clean.astype(object)
        .where(pd.notnull(df_clean), None)
        .to_dict(orient='records')
    )
    # Serializa/deserializa para asegurar tipos nativos JSON
    records = json.loads(json.dumps(records))

    # Obtiene o crea la colección
    if coleccion not in db.list_collection_names():
        collection = db.create_collection(coleccion)
    else:
        collection = db.get_collection(coleccion)

    # Inserta solo si está vacía
    if collection.find_one() is None:
        collection.insert_many(records)
        print(f"✅ Insertados {len(records)} registros en '{coleccion}'.")
    else:
        print(f"ℹ️ La colección '{coleccion}' ya contiene datos. No se insertó nada.")