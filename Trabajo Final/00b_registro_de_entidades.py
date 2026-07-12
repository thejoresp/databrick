# Databricks notebook source
# MAGIC %md
# MAGIC # 00b – Registro de Entidades
# MAGIC
# MAGIC Define las constantes con los nombres del catálogo, los
# MAGIC schemas de cada capa (bronze, silver, gold) y el volume de aterrizaje.

# COMMAND ----------

CATALOG = "dev"
DOMAIN = "ads"
BRONZE_SCHEMA = f"bronze_{DOMAIN}"
SILVER_SCHEMA = f"silver_{DOMAIN}"
GOLD_SCHEMA = f"gold_{DOMAIN}"
LANDING_VOLUME = f"landing_{DOMAIN}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Registro Bronze
# MAGIC
# MAGIC Define el diccionario `BRONZE_ENTITIES`: para cada
# MAGIC entidad indica el nombre de su función generadora (definida en
# MAGIC `00_ads_data_generator`) y si es un evento (datos por fecha) o una
# MAGIC dimensión (datos estáticos).

# COMMAND ----------

BRONZE_ENTITIES = {
    "advertisers": {"generator_fn": "get_advertisers", "is_event": False},
    "campaigns":   {"generator_fn": "get_campaigns",   "is_event": False},
    "ads":         {"generator_fn": "get_ads",         "is_event": False},
    "impressions": {"generator_fn": "get_impressions", "is_event": True},
    "clicks":      {"generator_fn": "get_clicks",      "is_event": True},
    "conversions": {"generator_fn": "get_conversions", "is_event": True},
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Registro Silver
# MAGIC
# MAGIC Define el diccionario `SILVER_RULES` con las reglas de
# MAGIC limpieza de cada entidad: clave primaria, claves de deduplicación, casts de
# MAGIC tipos, transformaciones custom y validaciones de FK. Estas reglas las
# MAGIC aplica `05_transformacion_silver_entidad`.

# COMMAND ----------

SILVER_RULES = {
    "advertisers": {
        "primary_key": "id",
        "dedup_keys": ["id"],
        "tiebreaker": "id",
        "casts": {"id": "int"},
        "custom_transforms": ["fix_website_protocol"],
        "fk_checks": [],
    },
    "campaigns": {
        "primary_key": "id",
        "dedup_keys": ["id"],
        "tiebreaker": "id",
        "casts": {"id": "int", "advertiser_id": "int", "daily_budget_usd": "decimal(10,2)"},
        "custom_transforms": ["flag_zero_budget"],
        "fk_checks": [],
    },
    "ads": {
        "primary_key": "id",
        "dedup_keys": ["id"],
        "tiebreaker": "id",
        "casts": {"id": "int", "campaign_id": "int"},
        "custom_transforms": ["flag_missing_creative"],
        "fk_checks": [],
    },
    "impressions": {
        "primary_key": "id",
        "dedup_keys": ["id"],
        "tiebreaker": "id",
        "casts": {"id": "int", "ad_id": "int", "cost_usd": "decimal(10,6)", "timestamp": "timestamp"},
        "custom_transforms": ["flag_zero_cost"],
        "fk_checks": [],
    },
    "clicks": {
        "primary_key": "id",
        "dedup_keys": ["id"],
        "tiebreaker": "id",
        "casts": {"id": "int", "impression_id": "int", "cost_usd": "decimal(10,4)", "timestamp": "timestamp"},
        "custom_transforms": [],
        "fk_checks": [("impressions", "id", "impression_id")],
    },
    "conversions": {
        "primary_key": "id",
        "dedup_keys": ["id"],
        "tiebreaker": "id",
        "casts": {"id": "int", "click_id": "int", "value_usd": "decimal(10,2)", "timestamp": "timestamp"},
        "custom_transforms": [],
        "fk_checks": [("clicks", "id", "click_id")],
    },
}