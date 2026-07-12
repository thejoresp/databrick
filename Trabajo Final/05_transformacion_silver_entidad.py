# Databricks notebook source
# MAGIC %md
# MAGIC # 05 – Ejecutor de Transformación Silver (una entidad)
# MAGIC
# MAGIC Recibe el nombre de una entidad por widget y aplica la limpieza definida en
# MAGIC `SILVER_RULES`: deduplicación, validación de FK, casts de tipos,
# MAGIC transformaciones custom y normalización de strings, terminando con un
# MAGIC `MERGE` a la tabla silver. La invoca `06_orquestador_carga_silver`.
# MAGIC
# MAGIC Primero crea el widget `entity_name`, importa el registro de entidades,
# MAGIC carga las funciones de Spark necesarias y valida que la entidad tenga
# MAGIC reglas definidas en `SILVER_RULES`.

# COMMAND ----------

dbutils.widgets.text("entity_name", "advertisers", "Entidad a procesar")
entity_name = dbutils.widgets.get("entity_name")
print(f"Procesando entidad (silver): {entity_name}")

# COMMAND ----------

# MAGIC %run ./00b_registro_de_entidades

# COMMAND ----------

from pyspark.sql.functions import (
    col, lit, when, trim, row_number, regexp_extract
)
from pyspark.sql.window import Window
from delta.tables import DeltaTable

if entity_name not in SILVER_RULES:
    raise ValueError(
        f"'{entity_name}' no está definida en SILVER_RULES. "
        f"Entidades válidas: {list(SILVER_RULES.keys())}"
    )

rules = SILVER_RULES[entity_name]
bronze_table = f"{CATALOG}.{BRONZE_SCHEMA}.{entity_name}"
silver_table = f"{CATALOG}.{SILVER_SCHEMA}.{entity_name}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Leer bronze
# MAGIC
# MAGIC Lee la tabla bronze de la entidad y muestra cuántas
# MAGIC filas tiene.

# COMMAND ----------

df = spark.table(bronze_table)
print(f"  bronze.{entity_name}: {df.count():,} filas leídas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Deduplicación
# MAGIC
# MAGIC Elimina duplicados: numera las filas con `row_number()`
# MAGIC particionando por `dedup_keys` y ordenando por `tiebreaker`, y conserva
# MAGIC solo la primera fila de cada grupo. Al final informa cuántos duplicados se
# MAGIC eliminaron.

# COMMAND ----------

dedup_keys = rules["dedup_keys"]
tiebreaker = rules["tiebreaker"]

window_dedup = Window.partitionBy(*dedup_keys).orderBy(col(tiebreaker).asc())

rows_before = df.count()
df = (
    df
    .withColumn("_rn", row_number().over(window_dedup))
    .filter(col("_rn") == 1)
    .drop("_rn")
)
rows_after = df.count()

print(f"  Duplicados eliminados: {rows_before - rows_after} (clave: {dedup_keys})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Validación de integridad referencial (FK checks)
# MAGIC
# MAGIC Valida cada FK declarada en el registro con un left join
# MAGIC contra la tabla padre en silver. Las filas sin coincidencia no se eliminan:
# MAGIC se marcan con una columna `is_orphan_<columna>` en `true`, y se informa
# MAGIC cuántas huérfanas se detectaron.

# COMMAND ----------

for parent_table, parent_key, child_fk_col in rules.get("fk_checks", []):
    parent_full_name = f"{CATALOG}.{SILVER_SCHEMA}.{parent_table}"
    valid_ids = (
        spark.table(parent_full_name)
        .select(col(parent_key).alias("_valid_id"))
        .distinct()
    )

    flag_col = f"is_orphan_{child_fk_col}"
    df = (
        df.join(valid_ids, df[child_fk_col] == valid_ids["_valid_id"], "left")
        .withColumn(flag_col, col("_valid_id").isNull())
        .drop("_valid_id")
    )

    orphan_count = df.filter(col(flag_col)).count()
    print(f"  FK check {child_fk_col} -> {parent_table}.{parent_key}: "
          f"{orphan_count} huérfanas marcadas en {flag_col}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Casts de tipos
# MAGIC
# MAGIC Convierte cada columna al tipo declarado en el registro:
# MAGIC `timestamp` para fechas, `decimal` para montos e `int` para IDs.

# COMMAND ----------

for column_name, target_type in rules.get("casts", {}).items():
    df = df.withColumn(column_name, col(column_name).cast(target_type))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Transforms custom (registradas por nombre)
# MAGIC
# MAGIC Define las cuatro transformaciones específicas por
# MAGIC entidad (completar el protocolo del website, flaggear presupuesto cero,
# MAGIC creative faltante y costo cero) y aplica solo las que el registro declara
# MAGIC para la entidad procesada.

# COMMAND ----------

def fix_website_protocol(df):
    # advertisers.website: ~2% sin "https://". Se agrega el protocolo si falta
    # (detectado por ausencia del patrón "http"). No se trata como nulo porque
    # el dominio en sí es válido — es solo un formato inconsistente.
    from pyspark.sql.functions import concat
    return df.withColumn(
        "website",
        when(
            ~col("website").rlike("^https?://"),
            concat(lit("https://"), col("website"))
        ).otherwise(col("website"))
    )

def flag_zero_budget(df):
    # campaigns.daily_budget_usd = 0.0 en ~1% de los casos. Se flaggea en vez
    # de eliminarse: la campaña sigue teniendo ads e impresiones asociadas y
    # perderla rompería los joins de gold.
    return df.withColumn("has_zero_budget", col("daily_budget_usd") == 0)

def flag_missing_creative(df):
    # ads.creative_url nulo en ~1% de los casos: nulo genuino (creative no
    # subido), se conserva la fila con un flag explícito en vez de imputar
    # una URL falsa.
    return df.withColumn("has_creative_url", col("creative_url").isNotNull())

def flag_zero_cost(df):
    # impressions.cost_usd = 0.0 en ~1%: puede ser una impresión house /
    # no facturable. Se flaggea, no se descarta el evento.
    return df.withColumn("is_zero_cost_impression", col("cost_usd") == 0)


CUSTOM_TRANSFORMS = {
    "fix_website_protocol": fix_website_protocol,
    "flag_zero_budget": flag_zero_budget,
    "flag_missing_creative": flag_missing_creative,
    "flag_zero_cost": flag_zero_cost,
}

for transform_name in rules.get("custom_transforms", []):
    df = CUSTOM_TRANSFORMS[transform_name](df)
    print(f"  Aplicado: {transform_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Normalización de strings (genérica)
# MAGIC
# MAGIC Aplica `trim` a todas las columnas de tipo string para
# MAGIC eliminar espacios al inicio y al final.

# COMMAND ----------

string_columns = [f.name for f in df.schema.fields if str(f.dataType) == "StringType()"]
for c in string_columns:
    df = df.withColumn(c, trim(col(c)))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. MERGE idempotente a silver
# MAGIC
# MAGIC Escribe el resultado en silver: si la tabla no existe,
# MAGIC la crea con `saveAsTable`; si ya existe, hace un `MERGE` por la clave
# MAGIC primaria que actualiza las filas existentes e inserta las nuevas. Así,
# MAGIC re-ejecutar la notebook no duplica filas. Al final muestra el total de
# MAGIC filas y devuelve el resultado a la notebook que la invocó.

# COMMAND ----------

primary_key = rules["primary_key"]
table_exists = spark.catalog.tableExists(silver_table)

if not table_exists:
    df.write.format("delta").saveAsTable(silver_table)
    print(f"  Tabla {silver_table} creada ({df.count():,} filas)")
else:
    target = DeltaTable.forName(spark, silver_table)
    (
        target.alias("t")
        .merge(df.alias("s"), f"t.{primary_key} = s.{primary_key}")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    print(f"  MERGE ejecutado sobre {silver_table}")

# COMMAND ----------

row_count = spark.table(silver_table).count()
print(f"  {silver_table}: {row_count:,} filas totales")

dbutils.notebook.exit(f"{entity_name}: OK ({row_count} filas)")