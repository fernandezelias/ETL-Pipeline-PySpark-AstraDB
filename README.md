🌐 Disponible en: [Español](README.md) | [English](README_EN.md)

# 🧱 ETL_Cassandra_Astra

Este proyecto implementa un **pipeline ETL batch** sobre una **arquitectura Data Lake zonificada**, utilizando **PySpark** para el procesamiento distribuido y **DataStax Astra DB (Cassandra)** como sistema de almacenamiento escalable.  

El caso simula un escenario de **análisis de onboarding de usuarios en una fintech en Brasil**, con el objetivo de calcular métricas clave (Drop, Activación, Hábito y Setup) y habilitar análisis A/B Testing sobre cohortes de usuarios.

---

## 🧰 Stack Tecnológico

- **Apache Spark (PySpark)** → procesamiento distribuido  
- **DataStax Astra DB (Cassandra)** → almacenamiento distribuido vía Data API  
- **Pandas / Matplotlib** → análisis exploratorio y visualizaciones  
- **Arquitectura Data Lake zonificada** → Landing → Universal → Smart  

---

## 🧭 Arquitectura del pipeline

```
               ┌───────────────┐
               │   Landing     │   ← Datos crudos (usuarios, transacciones, onboarding)
               └──────┬────────┘
                      │
            Limpieza / Tipificación / Anonimización
                      │
               ┌──────▼────────┐
               │   Universal   │   ← Datos limpios, tipados y persistidos en Astra DB
               └──────┬────────┘
                      │
              Lectura + Casteo de esquema en Spark
                      │
               ┌──────▼────────┐
               │    Smart      │   ← Vistas temporales + análisis SQL + métricas
               └───────────────┘
```

---

## 📂 Estructura del repositorio

```
ETL_Cassandra_Astra/
│
├── etl_utils.py              # Funciones auxiliares reutilizables (limpieza, casteos, inserciones, etc.)
├── ETL_Cassandra_Astra.ipynb # Notebook principal con el pipeline completo
├── requirements.txt          # Dependencias del entorno
├── .env                      # Token de conexión a Astra DB (no versionar)
└── README.md
```

---

📂 **Carpeta de datos**
Por razones de privacidad y licencia, los datasets originales utilizados en este proyecto no están incluidos en el repositorio público.  
Para ejecutar el pipeline localmente, coloque los archivos CSV requeridos en la carpeta `data/`.

---

## 🧰 Requisitos técnicos

Este proyecto utiliza **PySpark con soporte Hive embebido**.  
Además de las librerías de `requirements.txt`, es necesario tener instalado **Java 17** y definir la variable de entorno `JAVA_HOME`.

En entornos Conda:

```bash
conda install -c conda-forge openjdk=17
conda env config vars set JAVA_HOME=%CONDA_PREFIX%
```

El archivo `.env` debe contener la variable `ASTRA_DB_TOKEN` con el token de autenticación de DataStax Astra.  
> ⚠️ Este archivo **no debe versionarse** por seguridad.

---

## 🚀 Ejecución

1. Clonar el repositorio e instalar dependencias:

```bash
git clone https://github.com/tu_usuario/ETL_Cassandra_Astra.git
cd ETL_Cassandra_Astra
pip install -r requirements.txt
```

2. Ejecutar el notebook `ETL_Cassandra_Astra.ipynb` en orden secuencial (Landing → Universal → Smart → Métricas).  
3. Verificar la conexión a Astra DB antes de correr las funciones de persistencia.

---

## 📊 Métricas obtenidas

| Métrica       | Descripción                                                         | Resultado |
|--------------|----------------------------------------------------------------------|-----------|
| **Drop**     | Usuarios que no regresan después del primer uso                      | 92 %     |
| **Activación** | Usuarios que realizaron una primera transacción en 30 días         | 0.8 %    |
| **Hábito**   | Usuarios que consolidaron uso recurrente                             | 11.8 %   |
| **Setup**    | Usuarios que realizaron configuraciones iniciales                    | 42.5 %   |

> Estas métricas se calculan íntegramente mediante **Spark SQL** sobre vistas temporales, habilitando análisis exploratorios y segmentaciones sin necesidad de escribir a disco.

---

## 📝 Aprendizajes clave

- Integración de PySpark con Astra DB utilizando Data API.  
- Modelado zonificado de un Data Lake y separación clara de etapas de limpieza vs análisis.  
- Uso de vistas temporales para análisis SQL sin necesidad de un metastore persistente.  
- Cálculo de métricas de comportamiento de usuarios a gran escala.


---

📄 Licencia
Este proyecto está bajo la licencia MIT.

---

✍️ Autor: Elías Fernández  
📧 Contacto: fernandezelias86@gmail.com  
🔗 LinkedIn: www.linkedin.com/in/eliasfernandez208