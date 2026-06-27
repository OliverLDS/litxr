#!/bin/zsh

set -euo pipefail

script_dir="$(cd -- "$(dirname -- "$0")" && pwd)"
sync_start_time="$(date '+%Y-%m-%d %H:%M:%S')"

for arg in "$@"; do
  if [ "$arg" = "-h" ] || [ "$arg" = "--help" ]; then
    Rscript "$script_dir/fetch_arxiv_by_collection.R" --help
    exit 0
  fi
done

fetch_json="$(Rscript "$script_dir/fetch_arxiv_by_collection.R" "$@")"
added_count="$(printf '%s' "$fetch_json" | python3 -c 'import json,sys
obj=json.load(sys.stdin)
for key in ("written","total_written","fetched"):
    val=obj.get(key)
    if isinstance(val, int):
        print(val)
        raise SystemExit(0)
    if isinstance(val, float):
        print(int(val))
        raise SystemExit(0)
print(0)')"

printf '%s\n' "$fetch_json"

if [ "$added_count" -gt 0 ]; then
  Rscript "$script_dir/sync_thin_ref_stores.R" --json-mtime-after "$sync_start_time"
fi
