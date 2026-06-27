#!/bin/zsh

set -euo pipefail

script_dir="$(cd -- "$(dirname -- "$0")" && pwd)"
sync_start_time="$(date '+%Y-%m-%d %H:%M:%S')"

for arg in "$@"; do
  if [ "$arg" = "-h" ] || [ "$arg" = "--help" ]; then
    Rscript "$script_dir/fetch_arxiv_ref_json_by_ids.R" --help
    exit 0
  fi
done

fetch_json="$(Rscript "$script_dir/fetch_arxiv_ref_json_by_ids.R" "$@")"
written_count="$(printf '%s' "$fetch_json" | python3 -c 'import json,sys; obj=json.load(sys.stdin); print(int(obj.get("written", 0)))')"

printf '%s\n' "$fetch_json"

if [ "$written_count" -gt 0 ]; then
  Rscript "$script_dir/sync_thin_ref_stores.R" --json-mtime-after "$sync_start_time"
fi
