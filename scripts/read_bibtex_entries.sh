#!/bin/zsh

set -eu

if [[ $# -eq 1 && ( "$1" == "-h" || "$1" == "--help" ) ]]; then
  cat <<'EOF'
Usage:
  scripts/read_bibtex_entries.sh --bibtex PATH [--no-linked-arxiv]

Options:
  --bibtex PATH   Input BibTeX file path.
  --no-linked-arxiv  Keep DOI ref_ids as DOI values instead of promoting them
                     to linked arXiv ref_ids.
  -h, --help      Show this help message.

Behavior:
  - The script is a thin wrapper around litxr::read_bibtex_entries().
  - Compact JSON is written to stdout.
EOF
  exit 0
fi

bibtex_path=""
prefer_linked_arxiv=1

while [[ $# -gt 0 ]]; do
  case "$1" in
    -h|--help)
      exec "$0" --help
      ;;
    --bibtex)
      if [[ $# -lt 2 ]]; then
        print -u2 "Missing value for --bibtex"
        exit 1
      fi
      bibtex_path="$2"
      shift 2
      ;;
    --no-linked-arxiv)
      prefer_linked_arxiv=0
      shift
      ;;
    --*)
      print -u2 "Unknown argument: $1"
      exit 1
      ;;
    *)
      print -u2 "Unexpected positional argument: $1"
      exit 1
      ;;
  esac
done

if [[ -z "$bibtex_path" ]]; then
  print -u2 "Missing --bibtex"
  exit 1
fi

Rscript - "$bibtex_path" "$prefer_linked_arxiv" <<'EOF'
args <- commandArgs(trailingOnly = TRUE)
bibtex_path <- args[[1]]
prefer_linked_arxiv <- identical(args[[2]], "1")

emit_json <- function(x) {
  cat(jsonlite::toJSON(x, auto_unbox = TRUE, null = "null", pretty = FALSE), "\n", sep = "")
}

options(error = function() {
  err <- trimws(geterrmessage())
  if (!nzchar(err)) err <- "Unknown error"
  emit_json(list(status = "error", error = err))
  quit(save = "no", status = 1L)
})

ref_ids <- litxr::read_bibtex_entries(bibtex_path, prefer_linked_arxiv = prefer_linked_arxiv)
emit_json(list(
  status = "ok",
  bibtex = normalizePath(bibtex_path, winslash = "/", mustWork = FALSE),
  ref_ids = ref_ids
))
EOF
