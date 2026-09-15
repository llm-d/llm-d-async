#!/usr/bin/env bash
# Sweep submit rates against one database and append a CSV row per run.
# Usage: sweep.sh <dsn> <csv> [rates...]
set -euo pipefail
DSN="$1"; CSV="$2"; shift 2
RATES=("$@"); [[ ${#RATES[@]} -gt 0 ]] || RATES=(1000 2000 4000 6000 8000 12000 16000 20000)
BENCH="${BENCH:-go run ./hack/sqlqueue-bench}"
DURATION="${DURATION:-30s}"
CONSUMERS="${CONSUMERS:-4}"
POLLERS="${POLLERS:-2}"
SUBMIT_BATCH="${SUBMIT_BATCH:-100}"
for r in "${RATES[@]}"; do
    echo "--- rate ${r} req/s"
    ${BENCH} --dsn "${DSN}" --rate "${r}" --duration "${DURATION}" \
        --consumers "${CONSUMERS}" --pollers "${POLLERS}" --submit-batch "${SUBMIT_BATCH}" \
        --csv "${CSV}" --label "rate-${r}" || echo "rate ${r}: backlog did not drain"
done
echo "results: ${CSV}"
