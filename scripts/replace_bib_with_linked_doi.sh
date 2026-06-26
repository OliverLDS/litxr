#!/bin/zsh

set -eu

if [[ $# -eq 1 && ( "$1" == "-h" || "$1" == "--help" ) ]]; then
  cat <<'EOF'
Usage:
  scripts/replace_bib_with_linked_doi.sh INPUT_BIB [OUTPUT_BIB]

Arguments:
  INPUT_BIB    Path to the source BibTeX file.
  OUTPUT_BIB   Optional path to the output BibTeX file. If omitted, the
               input file is rewritten in place.

Behavior:
  - Reads canonical ref_ids from INPUT_BIB using scripts/read_bibtex_entries.sh.
  - Writes those ref_ids to OUTPUT_BIB using scripts/write_bib_by_ref_ids.sh.
  - Uses the default modes of both wrapped scripts, so arXiv entries are
    rewritten with linked DOI BibTeX entries when a DOI link exists.
  - Compact JSON is written to stdout and reports which arXiv ids were
    converted.
EOF
  exit 0
fi

input_bibtex=""
output_bibtex=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    -h|--help)
      exec "$0" --help
      ;;
    --*)
      print -u2 "Unknown argument: $1"
      exit 1
      ;;
    *)
      if [[ -z "$input_bibtex" ]]; then
        input_bibtex="$1"
      elif [[ -z "$output_bibtex" ]]; then
        output_bibtex="$1"
      else
        print -u2 "Unexpected positional argument: $1"
        exit 1
      fi
      shift
      ;;
  esac
done

if [[ -z "$input_bibtex" ]]; then
  print -u2 "Missing input BibTeX path"
  exit 1
fi

if [[ -z "$output_bibtex" ]]; then
  output_bibtex="$input_bibtex"
fi
in_place=0
if [[ "$output_bibtex" == "$input_bibtex" ]]; then
  in_place=1
fi

script_dir="${0:A:h}"
read_script="$script_dir/read_bibtex_entries.sh"
write_script="$script_dir/write_bib_by_ref_ids.sh"

tmpdir="${TMPDIR:-/tmp}"
read_json="$(mktemp "$tmpdir/litxr_read_bibtex.XXXXXX.json")"
write_json="$(mktemp "$tmpdir/litxr_write_bibtex.XXXXXX.json")"
output_tmp="$output_bibtex"
if [[ "$in_place" -eq 1 ]]; then
  output_tmp="$(mktemp "$tmpdir/litxr_replace_bib.XXXXXX.bib")"
fi

cleanup() {
  rm -f "$read_json" "$write_json"
  if [[ "$in_place" -eq 1 ]]; then
    rm -f "$output_tmp"
  fi
}
trap cleanup EXIT INT TERM

emit_json() {
  Rscript - "$read_json" "$write_json" "$input_bibtex" "$output_bibtex" "$1" <<'EOF'
args <- commandArgs(trailingOnly = TRUE)
read_path <- args[[1]]
write_path <- args[[2]]
input_bibtex <- args[[3]]
output_bibtex <- args[[4]]
status <- args[[5]]

emit <- function(x) {
  cat(jsonlite::toJSON(x, auto_unbox = TRUE, null = "null", pretty = FALSE), "\n", sep = "")
}

read_result <- tryCatch(jsonlite::fromJSON(read_path, simplifyVector = FALSE), error = function(e) NULL)
write_result <- tryCatch(jsonlite::fromJSON(write_path, simplifyVector = FALSE), error = function(e) NULL)

input_ids <- character()
resolved_ids <- character()
if (!is.null(read_result) && !is.null(read_result$ref_ids)) {
  input_ids <- as.character(read_result$ref_ids)
}
if (!is.null(write_result) && !is.null(write_result$resolved_ref_ids)) {
  resolved_ids <- as.character(write_result$resolved_ref_ids)
}

converted <- character()
n <- min(length(input_ids), length(resolved_ids))
if (n > 0L) {
  idx <- seq_len(n)
  converted <- input_ids[idx][
    grepl("^arxiv:", input_ids[idx], ignore.case = TRUE) &
      grepl("^doi:", resolved_ids[idx], ignore.case = TRUE)
  ]
  converted <- converted[!duplicated(converted)]
}

output <- list(
  status = status,
  input_bibtex = normalizePath(input_bibtex, winslash = "/", mustWork = FALSE),
  output_bibtex = normalizePath(output_bibtex, winslash = "/", mustWork = FALSE),
  ref_ids = input_ids,
  converted_arxiv_ids = converted
)
emit(output)
EOF
}

if ! "$read_script" --bibtex "$input_bibtex" > "$read_json"; then
  if [[ -s "$read_json" ]]; then
    cat "$read_json"
  else
    emit_json "error"
  fi
  exit 1
fi

ref_ids_raw="$(Rscript - "$read_json" <<'EOF'
args <- commandArgs(trailingOnly = TRUE)
result <- jsonlite::fromJSON(args[[1]], simplifyVector = FALSE)
ids <- result$ref_ids
if (is.null(ids) || !length(ids)) {
  cat("")
} else {
  cat(paste(as.character(ids), collapse = ","))
}
EOF
)"

if [[ -z "$ref_ids_raw" ]]; then
  : > "$output_tmp"
  if [[ "$output_tmp" != "$output_bibtex" ]]; then
    mv -f "$output_tmp" "$output_bibtex"
  fi
  emit_json "ok"
  exit 0
fi

if ! "$write_script" --output "$output_tmp" --ref-ids "$ref_ids_raw" > "$write_json"; then
  if [[ -s "$write_json" ]]; then
    cat "$write_json"
  else
    emit_json "error"
  fi
  exit 1
fi

written_ref_ids_count="$(Rscript - "$write_json" <<'EOF'
args <- commandArgs(trailingOnly = TRUE)
result <- jsonlite::fromJSON(args[[1]], simplifyVector = FALSE)
ids <- result$resolved_ref_ids
if (is.null(ids)) {
  cat("0")
} else {
  cat(as.character(length(ids)))
}
EOF
)"

if [[ "$in_place" -eq 1 && "$written_ref_ids_count" == "0" ]]; then
  Rscript - "$input_bibtex" "$output_bibtex" "$read_json" "$write_json" <<'EOF'
args <- commandArgs(trailingOnly = TRUE)
input_bibtex <- args[[1]]
output_bibtex <- args[[2]]
read_json <- args[[3]]
write_json <- args[[4]]

emit <- function(x) {
  cat(jsonlite::toJSON(x, auto_unbox = TRUE, null = "null", pretty = FALSE), "\n", sep = "")
}

read_result <- tryCatch(jsonlite::fromJSON(read_json, simplifyVector = FALSE), error = function(e) NULL)
write_result <- tryCatch(jsonlite::fromJSON(write_json, simplifyVector = FALSE), error = function(e) NULL)
emit(list(
  status = "error",
  input_bibtex = normalizePath(input_bibtex, winslash = "/", mustWork = FALSE),
  output_bibtex = normalizePath(output_bibtex, winslash = "/", mustWork = FALSE),
  error = "No BibTeX entries were written; leaving the input file unchanged.",
  ref_ids = if (!is.null(read_result$ref_ids)) as.character(read_result$ref_ids) else character(),
  converted_arxiv_ids = character(),
  resolved_ref_ids = if (!is.null(write_result$resolved_ref_ids)) as.character(write_result$resolved_ref_ids) else character()
))
EOF
  exit 1
fi

if [[ "$in_place" -eq 1 ]]; then
  mv -f "$output_tmp" "$output_bibtex"
fi

emit_json "ok"
