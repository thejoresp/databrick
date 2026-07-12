# Databricks notebook source
# MAGIC %md
# MAGIC # 03 – Orquestador de Carga Bronze
# MAGIC
# MAGIC Recorre las entidades de `BRONZE_ENTITIES` y ejecuta `02_ingesta_entidad`
# MAGIC para cada una vía `dbutils.notebook.run`.
# MAGIC
# MAGIC Importa el registro de entidades y ejecuta la ingesta de cada entidad,
# MAGIC guardando el resultado de cada corrida. Si una entidad falla, el pipeline
# MAGIC se corta.

# COMMAND ----------

# MAGIC %run ./00b_registro_de_entidades

# COMMAND ----------

results = {}

for entity_name in BRONZE_ENTITIES:
    print(f"\n{'=' * 50}\nProcesando: {entity_name}\n{'=' * 50}")

    try:
        result = dbutils.notebook.run(
            "02_ingesta_entidad",
            timeout_seconds=1800,
            arguments={"entity_name": entity_name},
        )
        results[entity_name] = result
        print(f"  -> {result}")
    except Exception as e:
        results[entity_name] = f"ERROR: {e}"
        print(f"  -> ERROR: {e}")
        raise  # cortar el pipeline ante el primer fallo, no seguir con datos a medio cargar

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumen de la corrida
# MAGIC
# MAGIC Imprime el resultado de cada entidad procesada.

# COMMAND ----------

for entity_name, result in results.items():
    print(f"  {entity_name:>15}: {result}")