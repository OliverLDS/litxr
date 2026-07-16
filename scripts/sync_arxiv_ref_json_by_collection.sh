#!/bin/zsh

set -euo pipefail

script_dir="$(cd -- "$(dirname -- "$0")" && pwd)"
sync_start_time="$(date '+%Y-%m-%d %H:%M:%S')"

usage() {
  cat <<'EOF'
Usage:
  scripts/sync_arxiv_ref_json_by_collection.sh --collection COLLECTION_ID [--start YYYY-MM-DD --end YYYY-MM-DD]

Options:
  --collection ID    arXiv-backed collection id to sync.
  --start DATE       Inclusive start date for arXiv submittedDate filtering.
  --end DATE         Inclusive end date for arXiv submittedDate filtering.
  --page-size N      Page size for arXiv API calls.
  --sleep-seconds S  Delay between arXiv requests.
  --search-query Q   Override the configured arXiv search query.
  --force            Re-run days already recorded in the collection history.
  -h, --help         Show this help message.

Behavior:
  - Fetches arXiv ref JSON files for the requested collection.
  - If new JSON files are written, runs scripts/sync_thin_ref_stores.R with
    --json-mtime-after set to the wrapper start time.
EOF
}

for arg in "$@"; do
  if [ "$arg" = "-h" ] || [ "$arg" = "--help" ]; then
    usage
    exit 0
  fi
done

fetch_json="$(Rscript "$script_dir/fetch_arxiv_by_collection.R" "$@")"
added_count="$(printf '%s' "$fetch_json" | python3 -c 'import json,sys
text=sys.stdin.read().strip().splitlines()
if not text:
    print(0)
    raise SystemExit(0)
for line in reversed(text):
    line=line.strip()
    if not line:
        continue
    try:
        obj=json.loads(line)
    except Exception:
        continue
    for key in ("written","total_written","fetched"):
        val=obj.get(key)
        if isinstance(val, int):
            print(val)
            raise SystemExit(0)
        if isinstance(val, float):
            print(int(val))
            raise SystemExit(0)
    break
print(0)')"

printf '%s\n' "$fetch_json"

if [ "$added_count" -gt 0 ]; then
  Rscript "$script_dir/sync_thin_ref_stores.R" --json-mtime-after "$sync_start_time"
fi
