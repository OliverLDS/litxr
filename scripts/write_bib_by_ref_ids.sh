#!/bin/zsh

set -eu

if [[ $# -eq 1 && ( "$1" == "-h" || "$1" == "--help" ) ]]; then
  cat <<'EOF'
Usage:
  scripts/write_bib_by_ref_ids.sh --output PATH --ref-ids REF1,REF2 [--config PATH] [--no-linked-doi] [--bibtex-key-overrides PATH] [--bibtex-field-overrides PATH]

Options:
  --output PATH       Target BibTeX file path.
  --ref-ids IDS       Comma-separated canonical ref_id values to export.
  --ref-id IDS        Alias for --ref-ids.
  --config PATH       Optional litxr config path or parsed config source.
  --no-linked-doi     Export arXiv ids without promoting to linked DOI rows.
  --bibtex-key-overrides PATH  Optional JSON object mapping canonical ref ids to keys.
  --bibtex-field-overrides PATH  Optional JSON object mapping canonical ref ids to field objects.
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
bibtex_key_overrides_path=""
bibtex_field_overrides_path=""

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
    --bibtex-key-overrides)
      if [[ $# -lt 2 ]]; then
        print -u2 "Missing value for --bibtex-key-overrides"
        exit 1
      fi
      bibtex_key_overrides_path="$2"
      shift 2
      ;;
    --bibtex-field-overrides)
      if [[ $# -lt 2 ]]; then
        print -u2 "Missing value for --bibtex-field-overrides"
        exit 1
      fi
      bibtex_field_overrides_path="$2"
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

if [[ -z "$output_path" ]]; then
  print -u2 "Missing --output"
  exit 1
fi
if [[ -z "$ref_ids_raw" ]]; then
  print -u2 "Missing --ref-ids"
  exit 1
fi

Rscript - "$output_path" "$ref_ids_raw" "$config_value" "$prefer_linked_doi" "$bibtex_key_overrides_path" "$bibtex_field_overrides_path" <<'EOF'
args <- commandArgs(trailingOnly = TRUE)
output_path <- args[[1]]
ref_ids_raw <- args[[2]]
config_value <- args[[3]]
prefer_linked_doi <- identical(args[[4]], "1")
bibtex_key_overrides_path <- args[[5]]
bibtex_field_overrides_path <- args[[6]]

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

bibtex_field_overrides <- NULL
if (!is.na(bibtex_field_overrides_path) && nzchar(bibtex_field_overrides_path)) {
  if (!file.exists(bibtex_field_overrides_path)) {
    stop("BibTeX field overrides file not found: ", bibtex_field_overrides_path, call. = FALSE)
  }
  bibtex_field_overrides <- jsonlite::fromJSON(bibtex_field_overrides_path, simplifyVector = FALSE)
  if (!is.list(bibtex_field_overrides) || is.null(names(bibtex_field_overrides))) {
    stop("BibTeX field overrides JSON must be an object of canonical ref ids to field objects.", call. = FALSE)
  }
}

bibtex_key_overrides <- NULL
if (!is.na(bibtex_key_overrides_path) && nzchar(bibtex_key_overrides_path)) {
  if (!file.exists(bibtex_key_overrides_path)) {
    stop("BibTeX key overrides file not found: ", bibtex_key_overrides_path, call. = FALSE)
  }
  raw_bibtex_key_overrides <- jsonlite::fromJSON(bibtex_key_overrides_path, simplifyVector = FALSE)
  bibtex_key_overrides <- as.character(unlist(raw_bibtex_key_overrides, use.names = FALSE))
  names(bibtex_key_overrides) <- names(raw_bibtex_key_overrides)
  if (!length(bibtex_key_overrides) || is.null(names(bibtex_key_overrides)) || length(bibtex_key_overrides) != length(names(raw_bibtex_key_overrides))) {
    stop("BibTeX key overrides JSON must be an object of canonical ref ids to keys.", call. = FALSE)
  }
}

result <- litxr::write_bibtex_entries(
  output_path,
  ref_ids,
  config = cfg,
  prefer_linked_doi = prefer_linked_doi,
  bibtex_key_overrides = bibtex_key_overrides,
  bibtex_field_overrides = bibtex_field_overrides
)
emit_json(result)
EOF
