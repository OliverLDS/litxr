#!/bin/zsh

set -eu
set -o pipefail

script_dir="${0:A:h}"
repo_root="${script_dir:h:h}"

usage() {
  cat <<'EOF'
Usage:
  scripts/human/report_arxiv_category_labels_md.sh [report_args...]

Purpose:
  Human-readable wrapper around scripts/report_arxiv_category_labels.R and
  scripts/filter_arxiv_category_labels_against_article_log.R and
  scripts/report_arxiv_category_labels_md.R.
  This wrapper pipes JSON output from the reporter into the filter, then into
  the markdown converter.

Notes:
  - All non-help arguments are forwarded to the reporter.
  - The reporter is forced to `--output-format json`.
  - The filter removes refs already present in the article log.
  - The converter renders markdown from stdin.
EOF
}

for arg in "$@"; do
  if [[ "$arg" == "-h" || "$arg" == "--help" ]]; then
    usage
    exit 0
  fi
done

Rscript "$repo_root/scripts/report_arxiv_category_labels.R" "$@" --output-format json |
  Rscript "$repo_root/scripts/filter_arxiv_category_labels_against_article_log.R" |
  Rscript "$repo_root/scripts/report_arxiv_category_labels_md.R"
