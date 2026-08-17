#!/usr/bin/env bash
set -e
cd "$(dirname "$0")/src"
echo "== Seeding source =="
python3 source_db.py
echo "== CDC extraction =="
python3 cdc_extractor.py
echo "== Warehouse load =="
python3 warehouse_loader.py
echo "== Catalog =="
python3 catalog.py
echo "== Validations =="
python3 validations.py
echo "== Done =="
