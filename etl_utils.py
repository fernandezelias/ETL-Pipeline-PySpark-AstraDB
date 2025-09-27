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
from astrapy import DataAPIClient  # Librería oficial de DataStax para Astra DB


# --- Funciones ETL auxiliares ---
def insertar_si_vacia(df: pd.DataFrame, coleccion: str, db) -> None:
    """
    Inserta los datos de un DataFrame en una colección de Astra DB solo si la colección está vacía.
    """
    # Reemplaza NaN por None
    df_clean = df.where(pd.notnull(df), None)

    # Convierte fechas a string si aplica
    df_clean = df_clean.applymap(
        lambda x: x.strftime('%Y-%m-%d') if hasattr(x, 'strftime') else x
    )

    # Convierte a lista de dicts JSON-compatibles
    records = (
        df_clean.astype(object)
        .where(pd.notnull(df_clean), None)
        .to_dict(orient='records')
    )
    records = json.loads(json.dumps(records))

    # Crea u obtiene la colección
    if coleccion not in db.list_collection_names():
        collection = db.create_collection(coleccion)
    else:
        collection = db.get_collection(coleccion)

    # Inserta solo si está vacía
    if collection.find_one() is None:
        collection.insert_many(records)
        print(f"✅ Insertados {len(records)} registros en '{coleccion}'.")
    else:
        print(f"ℹ️ La colección '{coleccion}' ya contiene datos. No se hicieron inserciones.")