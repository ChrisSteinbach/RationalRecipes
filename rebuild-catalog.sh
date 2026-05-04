#!/usr/bin/env bash
# Catalog rebuild — wipe and re-run.
# Usage:
#   ./rebuild-catalog.sh                  # full corpus (days)
#   ./rebuild-catalog.sh --smoke          # bilingual pancake slice for prompt validation
#
# The pipeline writes to recipes.db, then export_catalog_json.py
# emits the static JSON manifest the PWA actually consumes (vwt.y43).
# Sync copies the JSON into web/public/.
set -euo pipefail

DB="${OUTPUT_DB:-output/catalog/recipes.db}"
JSON_OUT="${CATALOG_JSON:-output/catalog/catalog.json}"
SMOKE=false
if [[ "${1:-}" == "--smoke" ]]; then
    SMOKE=true
    shift
fi

# In smoke mode the variant population is tiny and almost certainly
# below the v1 production cut (n>=100). Drop the threshold to 1 so
# the spot-check JSON still has rows.
if $SMOKE; then
    EXPORT_MIN_RECIPES="${EXPORT_MIN_RECIPES:-1}"
else
    EXPORT_MIN_RECIPES="${EXPORT_MIN_RECIPES:-100}"
fi

echo "=== Wipe DB (keep schema) ==="
sqlite3 "$DB" <<'SQL'
DELETE FROM parsed_ingredient_lines;
DELETE FROM variant_ingredient_stats;
DELETE FROM variant_members;
DELETE FROM variant_sources;
DELETE FROM variants;
DELETE FROM query_runs;
SQL
echo "  cleared."

if $SMOKE; then
    # Bilingual pancake slice for prompt validation.
    # --title-filter is a substring match on L1 keys, so "pancake"
    # covers pancake/pancakes (English) and "pannkaka" covers
    # pannkaka/pannkakor (Swedish). Run the pipeline once per filter
    # so both languages land in the same DB before export.
    SMOKE_FILTERS=(pancake pannkaka)
    echo "=== Smoke-test mode: bilingual pancake variants ==="
    for f in "${SMOKE_FILTERS[@]}"; do
        echo "--- filter: $f ---"
        echo "=== Pass 1: LLM parse ==="
        python3 scripts/scrape_catalog.py --pass1-only --title-filter "$f" "$@"
        echo "=== Pass 2: cluster + write variants ==="
        python3 scripts/scrape_catalog.py --pass2-only --title-filter "$f" "$@"
        echo "=== Pass 3: display titles ==="
        python3 scripts/scrape_catalog.py --pass3-only --pass3-force --title-filter "$f" "$@"
    done
else
    echo "=== Pass 1: LLM parse ==="
    python3 scripts/scrape_catalog.py --pass1-only "$@"

    echo "=== Pass 2: cluster + write variants ==="
    python3 scripts/scrape_catalog.py --pass2-only "$@"

    echo "=== Pass 3: display titles ==="
    python3 scripts/scrape_catalog.py --pass3-only --pass3-force "$@"
fi

echo "=== Export catalog.json (min_recipes=$EXPORT_MIN_RECIPES) ==="
python3 scripts/export_catalog_json.py \
    --db "$DB" \
    --output "$JSON_OUT" \
    --min-recipes "$EXPORT_MIN_RECIPES"

echo "=== Sync to PWA ==="
node web/scripts/sync-catalog.mjs

echo "=== Done ==="
if $SMOKE; then
    echo ""
    echo "Spot-check queries:"
    echo "  sqlite3 $DB \"SELECT ingredient FROM (SELECT json_extract(parsed_json, '\$.ingredient') AS ingredient FROM parsed_ingredient_lines WHERE parsed_json IS NOT NULL) WHERE ingredient GLOB '*[åäöÅÄÖ]*' OR ingredient IN ('olja','smor','mjol','agg') LIMIT 20;\""
    echo "  sqlite3 $DB \"SELECT display_title FROM variants LIMIT 20;\""
    if command -v jq >/dev/null 2>&1; then
        echo ""
        echo "catalog.json summary:"
        echo "  variants: $(jq '.recipes | length' web/public/catalog.json)"
        echo "  first 5 titles:"
        jq -r '.recipes[:5][] | "    - \(.title) (n=\(.sample_size))"' web/public/catalog.json
    else
        echo "  (install jq for catalog.json spot-check)"
    fi
fi
