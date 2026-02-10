# 🧱 ETL_Cassandra_Astra

🌐 Disponible en [Español](README.md)

![Python](https://img.shields.io/badge/Python-3.11-blue.svg)
![Spark](https://img.shields.io/badge/PySpark-3.x-orange.svg)
![Cassandra](https://img.shields.io/badge/Database-Cassandra%20(Astra%20DB)-purple.svg)
![Data Lake](https://img.shields.io/badge/Architecture-Data%20Lake%20Zoned-blue.svg)
![Status](https://img.shields.io/badge/Status-Active-success.svg)
![License](https://img.shields.io/badge/License-MIT-green.svg)

This project implements a **batch ETL pipeline** within a **zoned Data Lake architecture**, using **PySpark** for distributed processing and **DataStax Astra DB (Cassandra)** as a scalable storage system.

The use case simulates a **user onboarding analysis scenario for a fintech company in Brazil**, aimed at calculating key behavioral metrics (Churn, Activation, Habit Formation, and Setup Completion) and enabling A/B testing across user cohorts.

---

## 🧰 Technology Stack

- **Apache Spark (PySpark)** → Distributed data processing  
- **DataStax Astra DB (Cassandra)** → Distributed storage via Data API  
- **Pandas / Matplotlib** → Exploratory analysis and visualizations  
- **Zoned Data Lake Architecture** → Landing → Universal → Smart  

---

## 🧭 Pipeline Architecture

```
               ┌───────────────┐
               │   Landing     │   ← Raw data (users, transactions, onboarding)
               └──────┬────────┘
                      │
        Cleaning / Typing / Anonymization
                      │
               ┌──────▼────────┐
               │   Universal   │   ← Cleaned and typed data persisted in Astra DB
               └──────┬────────┘
                      │
            Read + Schema casting in Spark
                      │
               ┌──────▼────────┐
               │    Smart      │   ← Temporary views + SQL analysis + metrics
               └───────────────┘
```

---

## 📂 Repository Structure

```
ETL_Cassandra_Astra/
│
├── etl_utils.py              # Helper functions (cleaning, casting, insertions, etc.)
├── ETL_Cassandra_Astra.ipynb # Main notebook containing the complete pipeline
├── requirements.txt          # Environment dependencies
├── .env                      # Astra DB token (not versioned)
└── README.md
```

---

📂 **Data folder**  
For privacy and licensing reasons, the original datasets used in this project are not included in the public repository.  
If you wish to run the pipeline locally, create a `data/` folder and place CSV files with the same structure as the original datasets inside it.

---

## 🧰 Technical Requirements

This project uses **PySpark in local mode** to create temporary views and run SQL queries on transformed data.  
In addition to the libraries listed in `requirements.txt`, **Java 17** must be installed and the `JAVA_HOME` environment variable defined.

For Conda environments:

```bash
conda install -c conda-forge openjdk=17
conda env config vars set JAVA_HOME=%CONDA_PREFIX%
```

The `.env` file must contain the `ASTRA_DB_TOKEN` variable with the DataStax Astra authentication token.  
> ⚠️ This file **must not be versioned** for security reasons.

---

## 🚀 Execution

1. Clone the repository and install dependencies:

```bash
git clone https://github.com/fernandezelias/ETL_Cassandra_Astra.git
cd ETL_Cassandra_Astra
pip install -r requirements.txt
```

2. Run the `ETL_Cassandra_Astra.ipynb` notebook sequentially (Landing → Universal → Smart → Metrics).  
3. Ensure the Astra DB connection is properly configured before running persistence functions.

---

## 📊 Metrics

| Metric                | Description                                                                 | Result |
|-----------------------|-----------------------------------------------------------------------------|---------|
| **Churn**             | Users who did not return after their first session                         | 92 %    |
| **Activation**        | Users who made their first transaction within 30 days                      | 0.8 %   |
| **Habit Formation**   | Users who established recurrent usage patterns                             | 11.8 %  |
| **Setup Completion**  | Users who completed initial configuration steps                            | 42.5 %  |

> All metrics are fully calculated through **Spark SQL** on temporary views, enabling exploratory analyses and segmentation without writing to disk.

---

## 📝 Key Learnings

- Integration of PySpark with Astra DB using Data API.  
- Implementation of a zoned Data Lake architecture with clear separation between cleaning and analysis stages.  
- Use of temporary views for SQL analytics without a persistent metastore.  
- Calculation of large-scale user behavior metrics using distributed processing.

---

📄 License  
This project is released under the MIT License.

---

## ✍️ Author
**Elías Fernández**  
📧 Contacto: fernandezelias86@gmail.com  
🔗 LinkedIn: [Profile](https://www.linkedin.com/in/eliasfernandez208)

---

📁 **Repository:** ETL_Cassandra_Astra
