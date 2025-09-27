# etl_utils.py

# --- Importación de librerías ---
import json
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql import SparkSession

# --- Cargar variables de entorno desde .env ---
import os
from dotenv import load_dotenv

# --- Conexión a Astra DB ---
from astrapy import DataAPIClient # Librería oficial de DataStax para Astra DB


def connect_to_astra():
    """
    Crea y devuelve una conexión a Astra DB utilizando credenciales almacenadas en un archivo .env.

    Basado en el snippet oficial de DataStax Astra.

    Requisitos:
    - Archivo .env en la raíz del proyecto con:
        ASTRA_DB_TOKEN="tu_token_aquí"
    """
    load_dotenv()
    token_astra = os.getenv("ASTRA_DB_TOKEN")
    if not token_astra:
        raise ValueError("No se encontró ASTRA_DB_TOKEN en el archivo .env")

    client = DataAPIClient(token_astra)
    db = client.get_database_by_api_endpoint(
        "https://35e119e7-5c60-46cd-9184-5472ad94217b-us-east-2.apps.astra.datastax.com",
        keyspace="data_users",
    )
    return db


# --- Funciones ETL auxiliares ---
def insertar_si_vacia(df: pd.DataFrame, coleccion: str, db) -> None:
    """
    Inserta los datos de un DataFrame en una colección de Astra DB solo si la colección está vacía.
    """
    df_clean = df.where(pd.notnull(df), None)
    df_clean = df_clean.applymap(
        lambda x: x.strftime('%Y-%m-%d') if hasattr(x, 'strftime') else x
    )

    records = (
        df_clean.astype(object)
        .where(pd.notnull(df_clean), None)
        .to_dict(orient='records')
    )
    records = json.loads(json.dumps(records))

    if coleccion not in db.list_collection_names():
        collection = db.create_collection(coleccion)
    else:
        collection = db.get_collection(coleccion)

    if collection.find_one() is None:
        collection.insert_many(records)
        print(f"✅ Insertados {len(records)} registros en '{coleccion}'.")
    else:
        print(f"ℹ️ La colección '{coleccion}' ya contiene datos. No se insertó nada.")