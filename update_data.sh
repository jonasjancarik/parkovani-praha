#!/usr/bin/env bash
# Rebuild processed datasets after downloading new TSK exports.
# process.py regenerates data_permits_by_zone.csv from the legacy building mapping,
# so the audited candidate from permit_zone_mapping is installed over it before
# join.py runs. Installation is refused unless every structural gate passes.
# Spatial review (distance, ties, fallback coordinates) is still a manual approval.
set -euo pipefail
cd "$(dirname "$0")"

# Buildings farther than this from every active zone are left out (see manifest).
MAX_DISTANCE_M="${MAX_DISTANCE_M:-500}"
# Export records with more permits than this are citywide buckets, not buildings.
MAX_BUILDING_PERMITS="${MAX_BUILDING_PERMITS:-5000}"
TMP=/tmp/parkovani-permit-candidate
mkdir -p "$TMP"

uv run python process.py
uv run python -m src.permit_zone_mapping audit-history \
  --candidate-output "$TMP/candidate.csv" \
  --mapping-output "$TMP/mapping.csv" \
  --manifest-output "$TMP/manifest.json" \
  --max-distance-m "$MAX_DISTANCE_M" \
  --max-building-permits "$MAX_BUILDING_PERMITS" \
  --dry-run --strict

cp "$TMP/candidate.csv" data/processed/data_permits_by_zone.csv
echo "Installed audited permit candidate (manifest: $TMP/manifest.json)"
uv run python join.py
