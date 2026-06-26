#!/bin/zsh

set -eu

if [[ $# -eq 1 && ( "$1" == "-h" || "$1" == "--help" ) ]]; then
  cat <<'EOF'
Usage:
  scripts/write_bib_by_ref_ids.sh --output PATH --ref-ids REF1,REF2 [--config PATH] [--no-linked-doi]

Options:
  --output PATH       Target BibTeX file path.
  --ref-ids IDS       Comma-separated canonical ref_id values to export.
  --ref-id IDS        Alias for --ref-ids.
  --config PATH       Optional litxr config path or parsed config source.
  --no-linked-doi     Export arXiv ids without promoting to linked DOI rows.
  -h, --help          Show this help message.

Behavior:
  - The script is a thin wrapper around litxr::write_bibtex_entries().
  - Progress logs are written to stderr; compact JSON is written to stdout.
EOF
  exit 0
fi

output_path=""
ref_ids_raw=""
config_value=""
prefer_linked_doi=1

while [[ $# -gt 0 ]]; do
  case "$1" in
    -h|--help)
      exec "$0" --help
      ;;
    --output)
      if [[ $# -lt 2 ]]; then
        print -u2 "Missing value for --output"
        exit 1
      fi
      output_path="$2"
      shift 2
      ;;
    --ref-ids|--ref-id)
      if [[ $# -lt 2 ]]; then
        print -u2 "Missing value for --ref-ids"
        exit 1
      fi
      ref_ids_raw="$2"
      shift 2
      ;;
    --config)
      if [[ $# -lt 2 ]]; then
        print -u2 "Missing value for --config"
        exit 1
      fi
      config_value="$2"
      shift 2
      ;;
    --no-linked-doi)
      prefer_linked_doi=0
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

if [[ -z "$output_path" ]]; then
  print -u2 "Missing --output"
  exit 1
fi
if [[ -z "$ref_ids_raw" ]]; then
  print -u2 "Missing --ref-ids"
  exit 1
fi

Rscript - "$output_path" "$ref_ids_raw" "$config_value" "$prefer_linked_doi" <<'EOF'
args <- commandArgs(trailingOnly = TRUE)
output_path <- args[[1]]
ref_ids_raw <- args[[2]]
config_value <- args[[3]]
prefer_linked_doi <- identical(args[[4]], "1")

emit_json <- function(x) {
  cat(jsonlite::toJSON(x, auto_unbox = TRUE, null = "null", pretty = FALSE), "\n", sep = "")
}

parse_ref_ids <- function(x) {
  x <- as.character(x)
  if (!length(x) || is.na(x[[1L]]) || !nzchar(x[[1L]])) {
    return(character())
  }
  ids <- trimws(strsplit(x[[1L]], ",", fixed = TRUE)[[1]])
  ids <- ids[nzchar(ids)]
  ids[!duplicated(ids)]
}

options(error = function() {
  err <- trimws(geterrmessage())
  if (!nzchar(err)) err <- "Unknown error"
  emit_json(list(status = "error", error = err))
  quit(save = "no", status = 1L)
})

ref_ids <- parse_ref_ids(ref_ids_raw)
cfg <- NULL
if (!is.na(config_value) && nzchar(config_value)) {
  cfg <- config_value
}

result <- litxr::write_bibtex_entries(
  output_path,
  ref_ids,
  config = cfg,
  prefer_linked_doi = prefer_linked_doi
)
emit_json(result)
EOF
