# Databricks notebook source
# MAGIC %md
# MAGIC # 01 – Setup Unity Catalog
# MAGIC
# MAGIC Crea la estructura de Unity Catalog para la capa bronze del dominio `ads`.
# MAGIC
# MAGIC Primero importa las constantes y el registro de entidades desde
# MAGIC `00b_registro_de_entidades`.

# COMMAND ----------

# MAGIC %run ./00b_registro_de_entidades

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Catálogo, schema y volume
# MAGIC
# MAGIC Crea el catálogo, el schema bronze y el volume de
# MAGIC aterrizaje usando `IF NOT EXISTS` (re-ejecutarla no genera errores ni
# MAGIC duplicados), y muestra los nombres creados.

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG} COMMENT 'Catálogo de desarrollo'")

spark.sql(f"""
    CREATE SCHEMA IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}
    COMMENT 'Capa bronze del dominio de publicidad digital (AdTech)'
""")

spark.sql(f"""
    CREATE VOLUME IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}.{LANDING_VOLUME}
    COMMENT 'Área de aterrizaje de archivos crudos del dominio ads'
""")

print(f"Catálogo:  {CATALOG}")
print(f"Schema:    {CATALOG}.{BRONZE_SCHEMA}")
print(f"Volume:    {CATALOG}.{BRONZE_SCHEMA}.{LANDING_VOLUME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Tablas bronze (genérico, desde el registro)
# MAGIC
# MAGIC Recorre `BRONZE_ENTITIES` y crea una tabla Delta vacía
# MAGIC por cada entidad. No se definen columnas: Databricks las detecta
# MAGIC automáticamente en la primera ingesta del archivo JSON.

# COMMAND ----------

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {BRONZE_SCHEMA}")

for entity_name in BRONZE_ENTITIES:
    spark.sql(f"CREATE TABLE IF NOT EXISTS {entity_name} USING DELTA")
    print(f"  OK  {CATALOG}.{BRONZE_SCHEMA}.{entity_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Verificación
# MAGIC
# MAGIC Lista las tablas creadas en el schema bronze.

# COMMAND ----------

display(spark.sql(f"SHOW TABLES IN {CATALOG}.{BRONZE_SCHEMA}"))