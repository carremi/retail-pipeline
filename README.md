# Retail Pipeline

Pipeline ETL multicanal que unifica datos de ventas de **5 plataformas** en un data warehouse con modelo dimensional (star schema) en PostgreSQL, listo para consumir desde Power BI, Tableau o Looker.

```
Shopify ──┐
MercadoLibre ──┤                    ┌─────────┐     ┌──────────┐     ┌──────────┐
Amazon ────────┼──▶ RAW (landing) ──▶ STAGING ──▶ CORE (star) ──▶ BI Views
Tiendanube ────┤    (verbatim)       (cleaned)    (dim + fact)    (Power BI)
POS (físico) ──┘
```

**Canales soportados:** Shopify (JSON), MercadoLibre (JSON), Amazon (TSV), Tiendanube (Excel), POS físico (PostgreSQL).

> Los datos incluidos son **100% sintéticos**, generados por simuladores que replican las particularidades reales de cada plataforma (formatos, monedas, errores de datos, duplicados, etc.).

---

## Quick Start

```bash
# 1. Clonar y configurar
git clone <repo-url> && cd retail-pipeline
cp .env.example .env          # editar credenciales si es necesario

# 2. Demo completa (DB + datos de ejemplo + pipeline)
make demo

# 3. Ver resultados
# pgAdmin: http://localhost:5050
# O conectarse directamente: psql -h localhost -U retail_user -d retail_pipeline
```

Para parar todo: `make down`

---

## Requisitos

- **Docker** y **Docker Compose** v2+
- (Opcional para desarrollo local) Python 3.12+, `pip install -e ".[dev]"`

---

## Arquitectura

### Capas del Data Warehouse

| Capa | Schema | Descripcion |
|------|--------|-------------|
| **Raw** | `raw.*` | Datos originales tal cual llegan (JSON, TEXT). Sin transformacion. |
| **Staging** | `staging.*` | Datos limpios, tipados, normalizados por canal. Schema uniforme. |
| **Core** | `core.*` | Modelo dimensional: `fact_sales` + dimensiones (`dim_platform`, `dim_product`, `dim_date`). |
| **Reference** | `reference.*` | Datos maestros: tipos de cambio (FX), mapeo de SKUs, mapeo de estados. |
| **Ops** | `ops.*` | Operacional: checkpoints, rows rechazados, SKUs sin mapear, historial de runs. |

### Pipeline (4 etapas)

1. **Extract** - Ingesta desde archivos (JSON, TSV, Excel) y tablas POS. Ejecucion en paralelo.
2. **Normalize** - Limpieza, conversion de moneda (PEN/USD), mapeo de estados, deduplicacion.
3. **Validate** - Validacion con Pydantic: reglas de negocio (precios, cantidades, fechas, monedas).
4. **Load** - Upsert en `core.fact_sales` via SQL puro (INSERT...SELECT con ON CONFLICT).

---

## Estructura del Proyecto

```
retail-pipeline/
├── docker-compose.yml       # Postgres + pgAdmin + app services
├── Dockerfile               # Imagen Python para el pipeline
├── Makefile                 # Comandos rapidos (make up, make pipeline, etc.)
├── pyproject.toml           # Dependencias y config del proyecto
├── .env.example             # Variables de entorno (copiar a .env)
│
├── sql/                     # DDL del data warehouse (ejecutar en orden)
│   ├── 01_init_schemas.sql  # Schemas + tablas raw + ops
│   ├── 02_staging_schema.sql
│   ├── 03_reference_data.sql
│   ├── 04_sku_mapping.sql
│   ├── 05_core_schema.sql   # Star schema (dims + fact)
│   └── 06_views_powerbi.sql # Vistas para BI
│
├── src/retail_pipeline/
│   ├── extractors/          # Ingesta por plataforma
│   │   ├── base.py          # Clase base con checkpoint y logging
│   │   ├── shopify.py
│   │   ├── mercadolibre.py
│   │   ├── amazon.py
│   │   ├── tiendanube.py
│   │   └── pos.py
│   ├── transformers/
│   │   ├── base_normalizer.py  # Clase base: flujo comun de normalizacion
│   │   ├── normalize_shopify.py
│   │   ├── normalize_mercadolibre.py
│   │   ├── normalize_amazon.py
│   │   ├── normalize_tiendanube.py
│   │   ├── normalize_pos.py
│   │   ├── cleaning.py      # Funciones puras de limpieza (SKU, money, dates, FX)
│   │   ├── validators.py    # Validacion Pydantic + reporte de rechazados
│   │   └── sku_mapper.py    # Mapeo de SKUs entre plataformas
│   ├── loaders/
│   │   └── fact_sales.py    # Carga a core.fact_sales (SQL puro)
│   ├── orchestration/
│   │   └── run_daily.py     # Orquestador: extract -> normalize -> validate -> load
│   └── utils/
│       ├── config.py        # Configuracion centralizada (.env)
│       ├── db.py            # Engine singleton + ejecutor de SQL
│       └── logging_setup.py # Console + rotating file logger
│
├── simulators/              # Generadores de datos sinteticos
│   ├── _catalog.py          # Catalogo maestro (30 productos, 5 categorias)
│   ├── _dirty.py            # Inyeccion de errores realistas
│   ├── gen_shopify.py
│   ├── gen_mercadolibre.py
│   ├── gen_amazon.py
│   ├── gen_tiendanube.py
│   └── gen_pos.py
│
├── scripts/                 # Utilidades
│   ├── seed_fx_rates.py     # Poblar tipos de cambio
│   ├── seed_sku_mapping.py  # Poblar mapeo de SKUs
│   ├── run_normalizer.py    # Correr un normalizer individual
│   ├── run_all_normalizers.py
│   └── run_daily.sh         # Wrapper bash para cron/launchd
│
├── tests/
│   ├── conftest.py          # Fixtures compartidos
│   ├── test_cleaning.py     # Tests de funciones de limpieza
│   ├── test_validators.py   # Tests de validacion Pydantic
│   └── test_sku_mapper_integration.py  # Tests de integracion (requiere DB)
│
└── data/
    ├── drops/               # Aqui van los archivos de entrada
    │   └── .gitkeep
    └── reference/
        └── .gitkeep
```

---

## Comandos Disponibles

| Comando | Descripcion |
|---------|-------------|
| `make up` | Levantar DB + pgAdmin con schemas inicializados |
| `make demo` | Demo completa: DB + datos de ejemplo + pipeline |
| `make down` | Parar todo |
| `make pipeline` | Ejecutar el pipeline ETL |
| `make gen-data` | Generar datos de ejemplo con simuladores |
| `make seed` | Poblar datos de referencia (FX, SKUs) |
| `make init-db` | Crear/recrear schemas de la base de datos |
| `make test` | Correr tests unitarios |
| `make lint` | Correr linter (ruff) |
| `make clean` | Limpiar todo (volumes, datos, caches) |

---

## Adaptar a tu Propio Negocio

### 1. Reemplazar datos de entrada

Coloca tus archivos en `data/drops/`:
- `shopify_orders.json` — export de la API de Shopify
- `mercadolibre_orders.json` — export de la API de MercadoLibre
- `amazon_fulfilled_shipments.tsv` — reporte de Amazon Seller Central
- `tiendanube_ventas.xlsx` — export de Tiendanube (hoja "Ventas")

Para POS: configura la conexion a tu base de datos POS en el extractor.

### 2. Configurar monedas

Edita `scripts/seed_fx_rates.py` o inserta tus tipos de cambio directamente en `reference.fx_rates`:
```sql
INSERT INTO reference.fx_rates (currency, rate_date, rate_to_usd)
VALUES ('MXN', '2024-03-15', 0.059);
```

Agrega la moneda al whitelist en `src/retail_pipeline/transformers/validators.py`:
```python
VALID_CURRENCIES = {"USD", "PEN", "MXN"}
```

### 3. Agregar un nuevo canal de venta

Gracias al patron `BaseNormalizer`, agregar un canal nuevo requiere solo 3 pasos:

**a) Crear el extractor** (`src/retail_pipeline/extractors/mi_canal.py`):
```python
class MiCanalExtractor(BaseExtractor):
    source_name = "mi_canal"
    def extract(self) -> int:
        # leer archivo/API -> insertar en raw.mi_canal_orders
        ...
```

**b) Crear el normalizer** (`src/retail_pipeline/transformers/normalize_mi_canal.py`):
```python
class MiCanalNormalizer(BaseNormalizer):
    PLATFORM = "mi_canal"
    RAW_TABLE = "raw.mi_canal_orders"
    RAW_COLUMNS = "raw_id, ..."
    STAGING_TABLE = "staging.mi_canal_sales"

    def extract_fields(self, raw_df):
        # Mapear columnas del raw al formato staging
        ...
```

**c) Agregar las tablas SQL** y registrar el canal en `core.dim_platform` y `reference.status_mapping`.

### 4. Mapeo de SKUs

Inserta tus SKUs en `reference.sku_mapping`:
```sql
INSERT INTO reference.sku_mapping (source_platform, platform_sku, sku_canonical, product_name, category, base_price_usd)
VALUES ('shopify', 'MI-SKU-001', 'PROD-001', 'Mi Producto', 'Mi Categoria', 29.90);
```

---

## Tech Stack

| Componente | Tecnologia |
|-----------|-----------|
| Lenguaje | Python 3.12 |
| Base de datos | PostgreSQL 15 |
| ORM / SQL | SQLAlchemy 2.0 + psycopg 3 |
| Transformaciones | pandas |
| Validacion | Pydantic v2 |
| Contenedores | Docker + Docker Compose |
| Testing | pytest |
| Linting | ruff |
| BI | Power BI / Tableau (via vistas SQL) |

---

## Licencia

MIT
