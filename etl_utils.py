# etl_utils.py

# --- Librerías estándar ---
import os
import json
import hashlib

# --- Librerías de terceros ---
from dotenv import load_dotenv
from astrapy import DataAPIClient # Librería oficial de Astra DB para Python

# --- PySpark ---
from pyspark.sql import SparkSession
from pyspark.sql.functions import *  # col, lit, when, sum, etc. para transformaciones de DataFrames

# --- Análisis y visualización ---
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns


# --- Funciones ETL auxiliares ---
def insertar_si_vacia(df: pd.DataFrame, coleccion: str, db):
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


def get_hash_value(input_str):
    """
    Calcula el valor hash de una cadena de entrada.

    Parameters:
        input_str (str): La cadena de entrada para la cual se calculará el hash.

    Returns:
        str: El valor hash calculado
    """
    cleaned_input = input_str.lower().strip()
    hash_object = hashlib.sha256(cleaned_input.encode())
    hash_value = hash_object.hexdigest()
    return hash_value


def hash_column(df, column_name):
    """
    Aplica el algoritmo "Hash" a los valores en una columna del DataFrame.

    Parameters:
        df (pd.DataFrame): El DataFrame que contiene los datos.
        column_name (str): El nombre de la columna que se va a hashear.

    Returns:
        pd.DataFrame: El DataFrame modificado con los valores hasheados en la columna especificada.
    """
    try:

        # Crear una nueva columna _hashed con los valores hasheados de la columna recibida
        df.loc[:, f"{column_name}_hashed"] = df[column_name].apply(
            lambda row: get_hash_value(row) 
            )
        return df

    except KeyError:
        print(f"La columna '{column_name}' no existe en el DataFrame.")
        return df