# Databricks notebook source
# MAGIC %md
# MAGIC # 02 – Ejecutor de Ingesta (una entidad)
# MAGIC
# MAGIC Recibe el nombre de una entidad por widget y ejecuta su ingesta completa:
# MAGIC genera los datos, los guarda crudos en el volume y los carga a bronze con
# MAGIC `COPY INTO`. La invoca `03_orquestador_carga_bronze`, una vez por entidad.
# MAGIC
# MAGIC Primero crea el widget `entity_name`, importa el registro de entidades y
# MAGIC el generador de datos, y valida que la entidad recibida exista en
# MAGIC `BRONZE_ENTITIES`.

# COMMAND ----------

dbutils.widgets.text("entity_name", "advertisers", "Entidad a procesar")
entity_name = dbutils.widgets.get("entity_name")
print(f"Procesando entidad: {entity_name}")

# COMMAND ----------

# MAGIC %run ./00b_registro_de_entidades

# COMMAND ----------

# MAGIC %run ./00_ads_data_generator

# COMMAND ----------

if entity_name not in BRONZE_ENTITIES:
    raise ValueError(
        f"'{entity_name}' no está definida en BRONZE_ENTITIES. "
        f"Entidades válidas: {list(BRONZE_ENTITIES.keys())}"
    )

entity_config = BRONZE_ENTITIES[entity_name]
generator_fn = globals()[entity_config["generator_fn"]]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Generar datos
# MAGIC
# MAGIC Ejecuta la función generadora de la entidad y muestra
# MAGIC cuántos registros produjo.

# COMMAND ----------

data = generator_fn()
print(f"  {entity_name}: {len(data):,} registros generados")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Guardar crudo en el volume
# MAGIC
# MAGIC Escribe los registros en un archivo JSON Lines (un
# MAGIC objeto por línea) dentro del directorio de la entidad en el volume, con la
# MAGIC fecha de ejecución en el nombre del archivo.

# COMMAND ----------

import json
import os
from datetime import datetime, timezone

execution_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
entity_dir = f"/Volumes/{CATALOG}/{BRONZE_SCHEMA}/{LANDING_VOLUME}/{entity_name}"
os.makedirs(entity_dir, exist_ok=True)

filepath = f"{entity_dir}/{entity_name}_{execution_date}.json"
with open(filepath, "w", encoding="utf-8") as f:
    for record in data:
        f.write(json.dumps(record) + "\n")

print(f"  Archivo: {filepath}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. COPY INTO → tabla bronze
# MAGIC
# MAGIC Carga a la tabla bronze los archivos nuevos del
# MAGIC directorio de la entidad con `COPY INTO`. Como `COPY INTO` recuerda qué
# MAGIC archivos ya procesó, re-ejecutar no duplica datos. `mergeSchema = 'true'`
# MAGIC permite que la tabla se adapte si aparecen columnas nuevas. Al final se
# MAGIC muestra el total de filas y se devuelve el resultado a la notebook que la
# MAGIC invocó.

# COMMAND ----------

copy_into_sql = f"""
    COPY INTO {CATALOG}.{BRONZE_SCHEMA}.{entity_name}
    FROM '{entity_dir}'
    FILEFORMAT = JSON
    FORMAT_OPTIONS (
      'multiLine'   = 'false',
      'mergeSchema' = 'true'
    )
    COPY_OPTIONS (
      'mergeSchema' = 'true'
    )
"""

spark.sql(copy_into_sql)

row_count = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.{entity_name}").count()
print(f"  bronze.{entity_name}: {row_count:,} filas totales")

dbutils.notebook.exit(f"{entity_name}: OK ({row_count} filas)")