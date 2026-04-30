#!/bin/zsh

set -eu

if [[ $# -eq 1 && ( "$1" == "-h" || "$1" == "--help" ) ]]; then
  cat <<'EOF'
Usage:
  scripts/get_arxiv_abstract.sh <arxiv-id>

Options:
  -h, --help    Show this help message.

Notes:
  - Accepts either a bare arXiv id like 2405.03710 or a canonical id like
    arxiv:2405.03710.
  - The script expects exactly one matching local reference.
EOF
  exit 0
fi

if [[ $# -ne 1 ]]; then
  print -u2 "usage: $0 <arxiv-id>"
  exit 1
fi

if [[ "$1" == --* || "$1" == -?* ]]; then
  print -u2 "Unknown argument: $1"
  exit 1
fi

arxiv_id="${1}"

Rscript -e '
args <- commandArgs(trailingOnly = TRUE)
ref_id <- args[[1]]
if (!nzchar(ref_id)) {
  stop("One arXiv id is required.", call. = FALSE)
}
if (!grepl("^arxiv:", ref_id)) {
  ref_id <- paste0("arxiv:", ref_id)
}
hits <- litxr::litxr_find_refs(ref_id = ref_id)
if (!nrow(hits)) {
  stop("No record found for ", ref_id, ".", call. = FALSE)
}
if (nrow(hits) != 1L) {
  stop("Expected exactly one record for ", ref_id, " but found ", nrow(hits), ".", call. = FALSE)
}
abstract <- as.character(hits$abstract[[1]])
if (is.na(abstract) || !nzchar(trimws(abstract))) {
  stop("Abstract is missing for ", ref_id, ".", call. = FALSE)
}
cat(abstract, "\n", sep = "")
' -- "$arxiv_id"
