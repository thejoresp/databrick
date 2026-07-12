# Databricks notebook source
# MAGIC %md
# MAGIC # 06 – Orquestador de Transformación Silver
# MAGIC
# MAGIC Recorre las entidades de `SILVER_RULES` en orden y ejecuta
# MAGIC `05_transformacion_silver_entidad` para cada una. El orden importa: las FK
# MAGIC de `clicks` se validan contra `impressions`, y las de `conversions` contra
# MAGIC `clicks`, por lo que las tablas padre deben procesarse primero.
# MAGIC
# MAGIC Importa el registro de entidades y ejecuta la transformación de cada
# MAGIC entidad, guardando el resultado de cada corrida. Si una entidad falla,
# MAGIC el pipeline se corta.

# COMMAND ----------

# MAGIC %run ./00b_registro_de_entidades

# COMMAND ----------

results = {}

for entity_name in SILVER_RULES:
    print(f"\n{'=' * 50}\nTransformando a silver: {entity_name}\n{'=' * 50}")

    try:
        result = dbutils.notebook.run(
            "05_transformacion_silver_entidad",
            timeout_seconds=1800,
            arguments={"entity_name": entity_name},
        )
        results[entity_name] = result
        print(f"  -> {result}")
    except Exception as e:
        results[entity_name] = f"ERROR: {e}"
        print(f"  -> ERROR: {e}")
        # Se corta el pipeline: si una entidad falla, las siguientes podrían
        # depender de ella vía fk_checks (ver razonamiento arriba), así que
        # seguir procesando produciría validaciones de FK incorrectas.
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumen de la corrida
# MAGIC
# MAGIC Imprime el resultado de cada entidad procesada.

# COMMAND ----------

for entity_name, result in results.items():
    print(f"  {entity_name:>15}: {result}")