#!/bin/bash
set -euo pipefail

COMMAND="${1:-shell}"

case "$COMMAND" in
    init-db)
        echo "=== Initializing database schemas ==="
        for f in sql/01_init_schemas.sql \
                 sql/02_staging_schema.sql \
                 sql/03_reference_data.sql \
                 sql/04_sku_mapping.sql \
                 sql/05_core_schema.sql \
                 sql/06_views_powerbi.sql; do
            echo "  Running $f..."
            python -c "
from retail_pipeline.utils.db import run_sql_file
from pathlib import Path
run_sql_file(Path('$f'))
"
        done
        echo "=== Database initialized ==="
        ;;

    generate-data)
        echo "=== Generating sample data ==="
        python simulators/gen_shopify.py
        python simulators/gen_mercadolibre.py
        python simulators/gen_amazon.py
        python simulators/gen_tiendanube.py
        python simulators/gen_pos.py
        echo "=== Sample data generated ==="
        ;;

    seed)
        echo "=== Seeding reference data ==="
        python scripts/seed_fx_rates.py
        python scripts/seed_sku_mapping.py
        echo "=== Reference data seeded ==="
        ;;

    pipeline)
        echo "=== Running pipeline ==="
        python -m retail_pipeline.orchestration.run_daily
        ;;

    normalize)
        PLATFORM="${2:-all}"
        if [ "$PLATFORM" = "all" ]; then
            python scripts/run_all_normalizers.py
        else
            python scripts/run_normalizer.py "$PLATFORM"
        fi
        ;;

    shell)
        exec bash
        ;;

    *)
        echo "Unknown command: $COMMAND"
        echo "Available: init-db, generate-data, seed, pipeline, normalize [platform], shell"
        exit 1
        ;;
esac
