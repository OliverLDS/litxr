#!/bin/zsh

set -eu

script_dir="${0:A:h}"
repo_root="${script_dir:h:h}"

usage() {
  cat <<'EOF'
Usage:
  scripts/human/report_arxiv_category_labels_md.sh [report_args...]

Purpose:
  Human-readable wrapper around scripts/report_arxiv_category_labels.R.
  This wrapper forces `--output-format md`.

Notes:
  - All non-help arguments are forwarded to the reporter.
  - The reporter itself remains the JSON node for machine workflows.
EOF
}

for arg in "$@"; do
  if [[ "$arg" == "-h" || "$arg" == "--help" ]]; then
    usage
    exit 0
  fi
done

Rscript "$repo_root/scripts/report_arxiv_category_labels.R" "$@" --output-format md
