#!/bin/zsh

set -eu

if [[ $# -eq 1 && ( "$1" == "-h" || "$1" == "--help" ) ]]; then
  cat <<'EOF'
Usage:
  scripts/human/enrich_arxiv_with_doi.sh --arxiv-id ARXIV_ID --doi DOI

Options:
  --arxiv-id ID  Bare arXiv id or canonical arXiv ref_id.
  --doi DOI      Bare DOI, canonical doi: ref_id, or DOI URL.
  -h, --help     Show this help message.

Behavior:
  - Normalizes both inputs to canonical ref_ids.
  - Fails if either arXiv id or DOI already exists in ref_identity_map.fst.
  - Appends the pair only to ref_identity_map.fst.
  - Compact JSON is written to stdout.
EOF
  exit 0
fi

arxiv_id=""
doi=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    -h|--help)
      exec "$0" --help
      ;;
    --arxiv-id)
      if [[ $# -lt 2 ]]; then
        print -u2 "Missing value for --arxiv-id"
        exit 1
      fi
      arxiv_id="$2"
      shift 2
      ;;
    --doi)
      if [[ $# -lt 2 ]]; then
        print -u2 "Missing value for --doi"
        exit 1
      fi
      doi="$2"
      shift 2
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

if [[ -z "$arxiv_id" ]]; then
  print -u2 "Missing --arxiv-id"
  exit 1
fi
if [[ -z "$doi" ]]; then
  print -u2 "Missing --doi"
  exit 1
fi

Rscript - "$arxiv_id" "$doi" <<'EOF'
args <- commandArgs(trailingOnly = TRUE)
arxiv_id <- args[[1]]
doi <- args[[2]]

emit_json <- function(x) {
  cat(jsonlite::toJSON(x, auto_unbox = TRUE, null = "null", pretty = FALSE), "\n", sep = "")
}

options(error = function() {
  err <- trimws(geterrmessage())
  if (!nzchar(err)) err <- "Unknown error"
  emit_json(list(status = "error", error = err))
  quit(save = "no", status = 1L)
})

result <- litxr::litxr_add_ref_identity_pair(arxiv_id, doi)
emit_json(result)
EOF
