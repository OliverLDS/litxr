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
  - Parses INPUT_BIB once.
  - Resolves linked DOI substitutions through the thin scaffold stores.
  - Preserves original BibTeX keys for converted entries.
  - Preserves unresolved or unrecognized entries unchanged.
  - Compact JSON is written to stdout.
EOF
  exit 0
fi

input_bibtex=""
output_bibtex=""
script_dir="$(cd "$(dirname "$0")" && pwd)"
repo_root="$(cd "$script_dir/.." && pwd)"

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

Rscript - "$repo_root" "$input_bibtex" "$output_bibtex" <<'EOF'
args <- commandArgs(trailingOnly = TRUE)
repo_root <- args[[1L]]
input_bibtex <- args[[2L]]
output_bibtex <- args[[3L]]

emit <- function(x) {
  cat(jsonlite::toJSON(x, auto_unbox = TRUE, null = "null", pretty = FALSE), "\n", sep = "")
}

options(error = function() {
  err <- trimws(geterrmessage())
  if (!nzchar(err)) err <- "Unknown error"
  emit(list(status = "error", error = err))
  quit(save = "no", status = 1L)
})

pkgload::load_all(repo_root, quiet = TRUE, export_all = TRUE, helpers = FALSE, attach_testthat = FALSE)
result <- .litxr_replace_bib_with_linked_doi_file(
  input_bibtex = input_bibtex,
  output_bibtex = output_bibtex
)
emit(result)
EOF
