# Databricks notebook source
# MAGIC %md
# MAGIC # 07 – Gold: Performance de Campañas por Canal
# MAGIC
# MAGIC **Pregunta de negocio:** ¿qué campañas tienen mejor performance dentro de
# MAGIC su canal (Search, Social, Display, Video), medido por CTR, CVR, costo por
# MAGIC conversión y valor total generado?
# MAGIC
# MAGIC **Tablas silver utilizadas:** `campaigns`, `ads`, `impressions`, `clicks`,
# MAGIC `conversions`.
# MAGIC
# MAGIC Primero importa el registro de entidades y las funciones de Spark
# MAGIC necesarias.

# COMMAND ----------

# MAGIC %run ./00b_registro_de_entidades

# COMMAND ----------

from pyspark.sql.functions import col, count, sum as spark_sum, when, rank
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Leer silver y armar el embudo con LEFT JOIN
# MAGIC
# MAGIC Lee las cinco tablas silver (excluyendo las filas
# MAGIC huérfanas de `clicks` y `conversions`) y arma el embudo con LEFT JOIN
# MAGIC desde `campaigns` hacia abajo, para que las campañas sin eventos no
# MAGIC desaparezcan del resultado.

# COMMAND ----------

campaigns = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.campaigns")
ads = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.ads")
impressions = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.impressions")
clicks = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.clicks").filter(~col("is_orphan_impression_id"))
conversions = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.conversions").filter(~col("is_orphan_click_id"))

funnel = (
    campaigns.alias("c")
    .join(ads.alias("a"), col("a.campaign_id") == col("c.id"), "left")
    .join(impressions.alias("i"), col("i.ad_id") == col("a.id"), "left")
    .join(clicks.alias("cl"), col("cl.impression_id") == col("i.id"), "left")
    .join(conversions.alias("co"), col("co.click_id") == col("cl.id"), "left")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Agregaciones y métricas derivadas
# MAGIC
# MAGIC Agrupa por campaña y calcula los conteos de impresiones,
# MAGIC clicks y conversiones, los costos y el valor total; con eso deriva el CTR,
# MAGIC el CVR y el costo por conversión.

# COMMAND ----------

gold_df = (
    funnel
    .groupBy(
        col("c.id").alias("campaign_id"),
        col("c.campaign_name"),
        col("c.channel"),
        col("c.objective"),
    )
    .agg(
        count(col("i.id")).alias("impressions_count"),
        count(col("cl.id")).alias("clicks_count"),
        count(col("co.id")).alias("conversions_count"),
        spark_sum(col("i.cost_usd")).alias("total_impression_cost_usd"),
        spark_sum(col("cl.cost_usd")).alias("total_click_cost_usd"),
        spark_sum(col("co.value_usd")).alias("total_conversion_value_usd"),
    )
    .withColumn(
        "ctr",
        when(col("impressions_count") > 0, col("clicks_count") / col("impressions_count")).otherwise(0.0)
    )
    .withColumn(
        "cvr",
        when(col("clicks_count") > 0, col("conversions_count") / col("clicks_count")).otherwise(0.0)
    )
    .withColumn(
        "cost_per_conversion_usd",
        when(
            col("conversions_count") > 0,
            (col("total_impression_cost_usd") + col("total_click_cost_usd")) / col("conversions_count")
        ).otherwise(None)
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Window function: ranking dentro de cada canal
# MAGIC
# MAGIC Calcula el ranking de cada campaña dentro de su canal
# MAGIC con `rank()`, ordenando por valor total de conversión descendente.

# COMMAND ----------

window_channel = Window.partitionBy("channel").orderBy(col("total_conversion_value_usd").desc())

gold_df = gold_df.withColumn("rank_in_channel", rank().over(window_channel))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Guardar en gold (idempotente vía full overwrite)
# MAGIC
# MAGIC Guarda el resultado en la tabla gold con `overwrite`
# MAGIC completo: cada corrida recalcula la tabla entera desde silver, así que
# MAGIC re-ejecutar no duplica datos. Al final muestra la tabla ordenada por
# MAGIC canal y ranking.

# COMMAND ----------

gold_table = f"{CATALOG}.{GOLD_SCHEMA}.campaign_performance_by_channel"
gold_df.write.format("delta").mode("overwrite").saveAsTable(gold_table)

print(f"  {gold_table}: {gold_df.count():,} campañas")

# COMMAND ----------

display(
    spark.table(gold_table)
    .orderBy("channel", "rank_in_channel")
)