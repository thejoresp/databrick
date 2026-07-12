# Databricks notebook source
# MAGIC %md
# MAGIC # 08 – Gold: Revenue de Advertisers por Industria y País
# MAGIC
# MAGIC **Pregunta de negocio:** ¿qué advertisers generan más revenue dentro de su
# MAGIC industria, qué porcentaje del total de esa industria representa cada uno,
# MAGIC y cómo se distribuye el revenue entre industria y país?
# MAGIC
# MAGIC **Tablas silver utilizadas:** las 6 del dominio (`advertisers`,
# MAGIC `campaigns`, `ads`, `impressions`, `clicks`, `conversions`).
# MAGIC
# MAGIC Primero importa el registro de entidades y las funciones de Spark
# MAGIC necesarias.

# COMMAND ----------

# MAGIC %run ./00b_registro_de_entidades

# COMMAND ----------

from pyspark.sql.functions import col, when, count, sum as spark_sum, coalesce, lit
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Leer silver y armar el embudo
# MAGIC
# MAGIC Lee las seis tablas silver (excluyendo las filas
# MAGIC huérfanas de `clicks` y `conversions`) y arma el embudo con LEFT JOIN
# MAGIC desde `advertisers` hacia abajo, para que los advertisers sin eventos no
# MAGIC desaparezcan del resultado.

# COMMAND ----------

advertisers = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.advertisers")
campaigns = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.campaigns")
ads = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.ads")
impressions = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.impressions")
clicks = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.clicks").filter(~col("is_orphan_impression_id"))
conversions = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.conversions").filter(~col("is_orphan_click_id"))

funnel = (
    advertisers.alias("adv")
    .join(campaigns.alias("c"), col("c.advertiser_id") == col("adv.id"), "left")
    .join(ads.alias("a"), col("a.campaign_id") == col("c.id"), "left")
    .join(impressions.alias("i"), col("i.ad_id") == col("a.id"), "left")
    .join(clicks.alias("cl"), col("cl.impression_id") == col("i.id"), "left")
    .join(conversions.alias("co"), col("co.click_id") == col("cl.id"), "left")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Agregación base por advertiser
# MAGIC
# MAGIC Agrupa por advertiser y calcula el revenue total y la
# MAGIC cantidad de conversiones, reemplazando por 0 el revenue de los advertisers
# MAGIC sin conversiones.

# COMMAND ----------

revenue_by_advertiser = (
    funnel
    .groupBy(
        col("adv.id").alias("advertiser_id"),
        col("adv.advertiser_name"),
        col("adv.industry"),
        col("adv.country"),
    )
    .agg(
        spark_sum(col("co.value_usd")).alias("total_revenue_usd"),
        count(col("co.id")).alias("conversions_count"),
    )
    .withColumn("total_revenue_usd", coalesce(col("total_revenue_usd"), lit(0.0)))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Window function: participación dentro de la industria
# MAGIC
# MAGIC Calcula el revenue total de cada industria con una
# MAGIC window function y, con ese total, el porcentaje de participación de cada
# MAGIC advertiser dentro de su industria.

# COMMAND ----------

window_industry = Window.partitionBy("industry")

gold_df = revenue_by_advertiser.withColumn(
    "industry_total_revenue_usd",
    spark_sum("total_revenue_usd").over(window_industry)
).withColumn(
    "revenue_share_in_industry",
    when(
        col("industry_total_revenue_usd") != 0,
        col("total_revenue_usd") / col("industry_total_revenue_usd")
    ).otherwise(lit(0.0))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Guardar tabla principal
# MAGIC
# MAGIC Guarda la tabla principal en gold con `overwrite`
# MAGIC completo (cada corrida la recalcula entera desde silver, así re-ejecutar
# MAGIC no duplica datos). Después se muestra ordenada por industria y
# MAGIC participación.

# COMMAND ----------

gold_table = f"{CATALOG}.{GOLD_SCHEMA}.advertiser_revenue_by_industry"
gold_df.write.format("delta").mode("overwrite").saveAsTable(gold_table)

print(f"  {gold_table}: {gold_df.count():,} advertisers")

# COMMAND ----------

display(
    spark.table(gold_table)
    .orderBy(col("industry"), col("revenue_share_in_industry").desc())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Tabla complementaria: matriz pivot país × industria
# MAGIC
# MAGIC Construye la matriz de revenue por país × industria con
# MAGIC `pivot`, la guarda como segunda tabla gold y la muestra.

# COMMAND ----------

pivot_df = (
    revenue_by_advertiser
    .groupBy("country")
    .pivot("industry")
    .agg(spark_sum("total_revenue_usd"))
)

pivot_table = f"{CATALOG}.{GOLD_SCHEMA}.revenue_by_country_industry_pivot"
pivot_df.write.format("delta").mode("overwrite").saveAsTable(pivot_table)

print(f"  {pivot_table}: {pivot_df.count():,} países")
display(spark.table(pivot_table))