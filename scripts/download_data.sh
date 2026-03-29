#!/usr/bin/env bash
# Download external data sources for ingredient database:
#   1. USDA FoodData Central SR Legacy (portion weights, ~8K foods)
#   2. FAO/INFOODS Density Database v2.0 (density g/mL, ~6.5K entries)

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"

# --- USDA FDC SR Legacy ---
FDC_DIR="$ROOT_DIR/data/fdc"
FDC_URL="https://fdc.nal.usda.gov/fdc-datasets/FoodData_Central_sr_legacy_food_csv_2018-04.zip"
FDC_ZIP="$FDC_DIR/sr_legacy.zip"

mkdir -p "$FDC_DIR"

if [ -f "$FDC_DIR/food.csv" ] && [ -f "$FDC_DIR/food_portion.csv" ]; then
    echo "SR Legacy data already present in $FDC_DIR"
else
    echo "Downloading SR Legacy CSV from USDA FoodData Central..."
    curl -L -o "$FDC_ZIP" "$FDC_URL"

    echo "Extracting needed files..."
    for f in food.csv food_portion.csv measure_unit.csv; do
        unzip -o -j "$FDC_ZIP" "*/$f" -d "$FDC_DIR" 2>/dev/null || \
        unzip -o "$FDC_ZIP" "$f" -d "$FDC_DIR" 2>/dev/null || \
        echo "Warning: $f not found in archive"
    done
    rm -f "$FDC_ZIP"
    echo "SR Legacy: done"
fi

# --- FAO/INFOODS Density Database ---
FAO_DIR="$ROOT_DIR/data/fao"
FAO_URL="https://www.fao.org/fileadmin/templates/food_composition/documents/density_DB_v2_0_final-1__1_.xlsx"

mkdir -p "$FAO_DIR"

if [ -f "$FAO_DIR/density_db_v2.xlsx" ]; then
    echo "FAO/INFOODS density database already present in $FAO_DIR"
else
    echo "Downloading FAO/INFOODS Density Database v2.0..."
    curl -L -o "$FAO_DIR/density_db_v2.xlsx" "$FAO_URL"
    echo "FAO/INFOODS: done"
fi

echo ""
echo "All data downloaded. Run 'python3 scripts/build_db.py' to build ingredients.db"
