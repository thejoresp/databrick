# Databricks notebook source
# MAGIC %md
# MAGIC # 04 – Setup Silver y Gold
# MAGIC
# MAGIC Crea los schemas de las capas silver y gold. Las tablas no se crean acá:
# MAGIC nacen automáticamente en la primera ejecución de las notebooks de
# MAGIC transformación (silver) y de preguntas de negocio (gold).
# MAGIC
# MAGIC Importa el registro de entidades, crea ambos schemas con `IF NOT EXISTS`
# MAGIC y lista los schemas del catálogo.

# COMMAND ----------

# MAGIC %run ./00b_registro_de_entidades

# COMMAND ----------

spark.sql(f"""
    CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SILVER_SCHEMA}
    COMMENT 'Capa silver del dominio ads: datos limpios y normalizados por entidad'
""")

spark.sql(f"""
    CREATE SCHEMA IF NOT EXISTS {CATALOG}.{GOLD_SCHEMA}
    COMMENT 'Capa gold del dominio ads: tablas modeladas para preguntas de negocio específicas'
""")

print(f"Schema silver: {CATALOG}.{SILVER_SCHEMA}")
print(f"Schema gold:   {CATALOG}.{GOLD_SCHEMA}")

# COMMAND ----------

display(spark.sql(f"SHOW SCHEMAS IN {CATALOG}"))