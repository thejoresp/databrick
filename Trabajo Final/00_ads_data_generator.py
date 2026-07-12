# Databricks notebook source
# MAGIC %md
# MAGIC ## Dependencias
# MAGIC
# MAGIC La librería `faker` no viene instalada por defecto en los clusters de
# MAGIC Databricks

# COMMAND ----------

# MAGIC %pip install faker

# COMMAND ----------

# MAGIC %md
# MAGIC # Generador de datos ficticios – Dominio Ads / Publicidad Digital
# MAGIC
# MAGIC Esta notebook genera datos sintéticos pero estructuralmente realistas del
# MAGIC dominio de **publicidad digital (AdTech)**.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 1. ¿Cómo funciona la publicidad digital?
# MAGIC
# MAGIC En términos simplificados, el ecosistema funciona así:
# MAGIC
# MAGIC 1. Una **empresa (Advertiser)** quiere promocionar un producto o servicio.
# MAGIC 2. Define una **Campaign (campaña)** con: un objetivo, un presupuesto, un
# MAGIC    rango de fechas, uno o más canales (Search, Social, Display, Video).
# MAGIC 3. Dentro de cada campaña se crean **Ads (anuncios)**: creatividades
# MAGIC    específicas con una segmentación determinada.
# MAGIC 4. Cuando un usuario navega por una app o sitio, el sistema decide mostrar
# MAGIC    un anuncio. Ese evento se registra como una **Impression**.
# MAGIC 5. Si el usuario hace clic en el anuncio, se registra un **Click**.
# MAGIC 6. Si luego realiza una acción valiosa, se registra una **Conversion**.
# MAGIC
# MAGIC **Advertiser → Campaign → Ad → Impression → Click → Conversion**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 2. Modelo de datos
# MAGIC
# MAGIC Esta notebook modela el dominio con **6 entidades principales**:
# MAGIC
# MAGIC - **advertisers**: empresas que invierten en publicidad.
# MAGIC - **campaigns**: campañas asociadas a un advertiser.
# MAGIC - **ads**: creatividades individuales dentro de una campaña.
# MAGIC - **impressions**: evento — el anuncio fue mostrado.
# MAGIC - **clicks**: evento — el usuario hizo clic (subconjunto de impressions).
# MAGIC - **conversions**: evento — acción valiosa posterior al click (subconjunto
# MAGIC   de clicks).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Uso de las funciones
# MAGIC
# MAGIC Cada entidad tiene una función con prefijo `get_`. Funciones disponibles:
# MAGIC
# MAGIC - `get_advertisers()`
# MAGIC - `get_campaigns()`
# MAGIC - `get_ads()`
# MAGIC - `get_impressions()`
# MAGIC - `get_clicks()`
# MAGIC - `get_conversions()`
# MAGIC
# MAGIC ## Comportamiento general
# MAGIC
# MAGIC - Todas devuelven `list[dict]`.
# MAGIC - No tienen parámetros obligatorios.
# MAGIC - Son deterministas (mismos parámetros producen siempre el mismo resultado).
# MAGIC - El parámetro `seed` es opcional y tiene un valor por defecto fijo.
# MAGIC
# MAGIC ## Eventos (impressions, clicks, conversions)
# MAGIC
# MAGIC Parámetros disponibles: `start_date`, `end_date` (formato `"YYYY-MM-DD"`).
# MAGIC
# MAGIC - Si no se indican fechas: se generan datos correspondientes a **ayer (UTC)**.
# MAGIC - Si se indica solo `start_date`: se generan datos únicamente para ese día.
# MAGIC - Si se indican ambas fechas: se generan datos para todo el rango (inclusive).
# MAGIC
# MAGIC Modificar fechas produce un dataset distinto, siempre de forma determinista.

# COMMAND ----------

from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any
import random
import hashlib
from faker import Faker

DEFAULT_SEED = 2026

MAX_ADVERTISERS = 200
MAX_CAMPAIGNS = 2000
MAX_ADS = 500000

IMPRESSION_RATE = 0.01   # 1% del universo de ads por día
CTR = 0.02                 # 2%
CVR = 0.08                 # 8%


def _stable_seed(seed: int, *parts: Any) -> int:
    raw = "|".join([str(seed)] + [str(p) for p in parts]).encode()
    return int(hashlib.sha256(raw).hexdigest()[:8], 16)

def _rng(seed: int):
    return random.Random(seed)

def _faker(seed: int):
    fk = Faker("en_US")
    fk.seed_instance(seed)
    return fk

def _resolve_dates(start_date, end_date):
    today = datetime.now(timezone.utc).date()
    yesterday = today - timedelta(days=1)

    if start_date is None and end_date is None:
        return yesterday, yesterday

    if isinstance(start_date, str):
        start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
    if isinstance(end_date, str):
        end_date = datetime.strptime(end_date, "%Y-%m-%d").date()

    if start_date and not end_date:
        return start_date, start_date

    return start_date, end_date

# ----------------------
# DIMENSIONS
# ----------------------

def get_advertisers(seed: int = DEFAULT_SEED) -> List[Dict]:
    s = _stable_seed(seed, "advertisers")
    r = _rng(s)
    fk = _faker(s)
    out = []
    for i in range(1, MAX_ADVERTISERS + 1):
        website = f"https://{fk.domain_name()}"
        if r.random() < 0.02:  # 2% sin protocolo
            website = website.replace("https://", "")
        out.append({
            "id": i,
            "advertiser_name": fk.company(),
            "industry": r.choice(["Retail","FinTech","Gaming","Travel","SaaS"]),
            "website": website,
            "country": fk.country_code()
        })
    return out

def get_campaigns(seed: int = DEFAULT_SEED) -> List[Dict]:
    s = _stable_seed(seed, "campaigns")
    r = _rng(s)
    fk = _faker(s)
    out = []
    for i in range(1, MAX_CAMPAIGNS + 1):
        budget = round(r.uniform(50,3000),2)
        if r.random() < 0.01:  # 1% presupuesto en cero
            budget = 0.0
        out.append({
            "id": i,
            "advertiser_id": (i % MAX_ADVERTISERS) + 1,
            "campaign_name": fk.bs().title(),
            "channel": r.choice(["Search","Social","Display","Video"]),
            "objective": r.choice(["Awareness","Traffic","Conversions"]),
            "daily_budget_usd": budget
        })
    return out

def get_ads(seed: int = DEFAULT_SEED) -> List[Dict]:
    s = _stable_seed(seed, "ads")
    r = _rng(s)
    fk = _faker(s)
    out = []
    for i in range(1, MAX_ADS + 1):
        url = f"https://cdn.example.com/{fk.uuid4()}.png"
        if r.random() < 0.01:  # 1% creative inconsistente
            url = None
        out.append({
            "id": i,
            "campaign_id": (i % MAX_CAMPAIGNS) + 1,
            "format": r.choice(["Banner","Video","Native","SearchText"]),
            "creative_url": url
        })
    return out

# ----------------------
# EVENTS (pure percentage-based volume)
# ----------------------

def get_impressions(start_date=None, end_date=None, seed: int = DEFAULT_SEED):
    sd, ed = _resolve_dates(start_date, end_date)
    s = _stable_seed(seed, "impressions", sd, ed)
    r = _rng(s)
    fk = _faker(s)

    impressions_per_day = int(MAX_ADS * IMPRESSION_RATE)

    out = []
    current = sd
    id_counter = 1

    while current <= ed:
        for _ in range(impressions_per_day):
            ts = datetime.combine(current, datetime.min.time(), tzinfo=timezone.utc) + timedelta(
                seconds=r.randint(0, 86400)
            )

            cost = round(r.uniform(0.001, 0.02), 6)
            if r.random() < 0.01:
                cost = 0.0

            record = {
                "id": id_counter,
                "ad_id": r.randint(1, MAX_ADS),
                "timestamp": ts.isoformat(),
                "country": fk.country_code(),
                "cost_usd": cost
            }

            out.append(record)

            if r.random() < 0.005:
                out.append(record.copy())

            id_counter += 1

        current += timedelta(days=1)

    return out


def get_clicks(start_date=None, end_date=None, seed: int = DEFAULT_SEED):
    sd, ed = _resolve_dates(start_date, end_date)
    s = _stable_seed(seed, "clicks", sd, ed)
    r = _rng(s)

    impressions_per_day = int(MAX_ADS * IMPRESSION_RATE)
    clicks_per_day = int(impressions_per_day * CTR)

    out = []
    current = sd
    id_counter = 1

    while current <= ed:
        for _ in range(clicks_per_day):
            ts = datetime.combine(current, datetime.min.time(), tzinfo=timezone.utc) + timedelta(
                seconds=r.randint(0, 86400)
            )

            impression_id = r.randint(1, impressions_per_day)
            if r.random() < 0.005:
                impression_id = 9999999

            out.append({
                "id": id_counter,
                "impression_id": impression_id,
                "timestamp": ts.isoformat(),
                "cost_usd": round(r.uniform(0.1, 3.0), 4)
            })

            id_counter += 1

        current += timedelta(days=1)

    return out


def get_conversions(start_date=None, end_date=None, seed: int = DEFAULT_SEED):
    sd, ed = _resolve_dates(start_date, end_date)
    s = _stable_seed(seed, "conversions", sd, ed)
    r = _rng(s)

    impressions_per_day = int(MAX_ADS * IMPRESSION_RATE)
    clicks_per_day = int(impressions_per_day * CTR)
    conversions_per_day = int(clicks_per_day * CVR)

    out = []
    current = sd
    id_counter = 1

    while current <= ed:
        for _ in range(conversions_per_day):
            ts = datetime.combine(current, datetime.min.time(), tzinfo=timezone.utc) + timedelta(
                seconds=r.randint(0, 86400)
            )

            click_id = r.randint(1, clicks_per_day)
            if r.random() < 0.01:
                click_id = 8888888

            value = round(r.lognormvariate(3, 0.6), 2)

            out.append({
                "id": id_counter,
                "click_id": click_id,
                "timestamp": ts.isoformat(),
                "value_usd": value
            })

            id_counter += 1

        current += timedelta(days=1)

    return out