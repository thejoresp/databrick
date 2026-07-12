# Pipeline Ads — Arquitectura Medallion en Databricks

Trabajo final que implementa un **pipeline de datos completo sobre Databricks** para el dominio de **publicidad digital (AdTech)**, siguiendo la **arquitectura Medallion** (Bronze → Silver → Gold) con **Unity Catalog** y **Lakeflow Jobs**.

## De qué se trata

El proyecto modela el ecosistema de publicidad digital —anunciantes, campañas, anuncios y sus métricas de rendimiento (impresiones, clics, conversiones, revenue)— y construye un flujo de datos que va desde la generación de datos crudos hasta tablas analíticas listas para consumo.

## Arquitectura Medallion

| Capa | Propósito |
|------|-----------|
| **Bronze** | Ingesta de datos crudos tal cual llegan al *landing volume* de Unity Catalog. |
| **Silver** | Limpieza, tipado y normalización de las entidades del dominio. |
| **Gold** | Tablas agregadas de negocio: rendimiento de campañas e ingresos por anunciante. |

## Contenido

Notebooks de Databricks (carpeta `Trabajo Final/`):

- **`00_ads_data_generator.py`** — Genera datos sintéticos y realistas del dominio Ads con `faker`.
- **`00b_registro_de_entidades.py`** — Constantes y registro central de entidades del pipeline.
- **`01_configuracion_unity_catalog.py`** — Crea catálogo, schema bronze y volume de aterrizaje.
- **`02_ingesta_entidad.py`** — Lógica reutilizable de ingesta de una entidad a Bronze.
- **`03_orquestador_carga_bronze.py`** — Orquesta la carga de todas las entidades a Bronze.
- **`04_configuracion_silver_gold.py`** — Configura los schemas Silver y Gold.
- **`05_transformacion_silver_entidad.py`** — Transformación reutilizable de Bronze → Silver.
- **`06_orquestador_carga_silver.py`** — Orquesta la carga de la capa Silver.
- **`07_gold_performance_campanas.py`** — Tabla Gold de rendimiento de campañas.
- **`08_gold_revenue_anunciantes.py`** — Tabla Gold de ingresos por anunciante.
- **`pipeline_ads_medallion.yml`** — Definición del Job (Lakeflow) con las dependencias entre tareas y ejecución diaria.

## Orquestación

`pipeline_ads_medallion.yml` define un job diario que ejecuta las tareas respetando sus dependencias:

```
01_configuracion_unity_catalog
        └─> 03_carga_bronze
                └─> 04_configuracion_silver_gold
                        └─> 06_carga_silver
                                ├─> 07_gold_rendimiento_campanas
                                └─> 08_gold_ingresos_anunciantes
```

## Requisitos

- Workspace de **Databricks** con **Unity Catalog** habilitado.
- Librería `faker` (se instala dentro del notebook generador con `%pip install faker`).

## Uso

Importá la carpeta `Trabajo Final/` al workspace de Databricks y desplegá el job definido en `pipeline_ads_medallion.yml`, o ejecutá los notebooks en orden numérico manualmente.
