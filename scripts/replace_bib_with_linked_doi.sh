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
  - Converted entries keep their original BibTeX keys.
  - Unresolved or unrecognized BibTeX entries are preserved unchanged.
  - Article-type arXiv entries without a matching identity-map pair are
    reported as suspicious.
  - Compact JSON is written to stdout and reports which arXiv ids were
    converted, which DOI values replaced them, and which linked DOI values
    were missing from the local DOI store.
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

if ! "$read_script" --bibtex "$input_bibtex" > "$read_json"; then
  if [[ -s "$read_json" ]]; then
    cat "$read_json"
  else
    cat '{"status":"error"}'
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

if ! "$write_script" --output "$output_tmp" --ref-ids "$ref_ids_raw" > "$write_json"; then
  if [[ -s "$write_json" ]]; then
    cat "$write_json"
  else
    cat '{"status":"error"}'
  fi
  exit 1
fi

Rscript - "$write_json" "$input_bibtex" "$output_bibtex" "$output_tmp" <<'EOF'
args <- commandArgs(trailingOnly = TRUE)
write_path <- args[[1]]
input_bibtex <- args[[2]]
output_bibtex <- args[[3]]
output_tmp <- args[[4]]

emit <- function(x) {
  cat(jsonlite::toJSON(x, auto_unbox = TRUE, null = "null", pretty = FALSE), "\n", sep = "")
}

parse_entries <- function(path) {
  lines <- readLines(path, warn = FALSE)
  if (!length(lines)) {
    return(list(entries = list(), keys = character()))
  }

  entries <- list()
  keys <- character()
  types <- character()
  i <- 1L
  while (i <= length(lines)) {
    line <- lines[[i]]
    if (!grepl("^\\s*@", line)) {
      i <- i + 1L
      next
    }

    entry_type <- sub("^\\s*@([[:alnum:]]+)\\s*\\{.*$", "\\1", line, perl = TRUE)
    key <- sub("^\\s*@\\w+\\s*\\{\\s*([^,]+),.*$", "\\1", line, perl = TRUE)
    chunk <- character()
    depth <- 0L
    repeat {
      if (i > length(lines)) {
        break
      }
      current <- lines[[i]]
      chunk <- c(chunk, current)
      opens <- lengths(regmatches(current, gregexpr("\\{", current, perl = TRUE)))
      closes <- lengths(regmatches(current, gregexpr("\\}", current, perl = TRUE)))
      depth <- depth + opens - closes
      i <- i + 1L
      if (depth <= 0L) {
        break
      }
    }

    entries[[length(entries) + 1L]] <- chunk
    keys <- c(keys, key)
    types <- c(types, tolower(entry_type))
  }

  list(entries = entries, keys = keys, types = types)
}

write_result <- tryCatch(jsonlite::fromJSON(write_path, simplifyVector = FALSE), error = function(e) NULL)
unresolved_ids <- if (!is.null(write_result) && !is.null(write_result$unresolved_ref_ids)) as.character(write_result$unresolved_ref_ids) else character()

input_parsed <- parse_entries(input_bibtex)
output_parsed <- parse_entries(output_tmp)
input_entries <- input_parsed$entries
input_keys <- input_parsed$keys
input_types <- input_parsed$types
output_entries <- output_parsed$entries
cfg <- litxr::litxr_read_config()
link_maps <- litxr:::.litxr_bibtex_link_maps(cfg)
scaffold_cache <- litxr:::.litxr_bibtex_scaffold_cache(cfg)
identity_arxiv_ids <- character()
if (!is.null(link_maps) && !is.null(link_maps$arxiv_to_doi) && length(link_maps$arxiv_to_doi)) {
  identity_arxiv_ids <- unique(vapply(
    names(link_maps$arxiv_to_doi),
    litxr:::.litxr_bare_arxiv_id,
    character(1)
  ))
}
available_doi_ids <- character()
if (!is.null(scaffold_cache$doi) && nrow(scaffold_cache$doi) && "doi" %in% names(scaffold_cache$doi)) {
  available_doi_ids <- unique(vapply(
    as.character(scaffold_cache$doi$doi),
    litxr:::.litxr_bare_doi,
    character(1)
  ))
}
input_ids <- if (length(input_entries)) {
  vapply(seq_along(input_entries), function(i) {
    litxr:::.litxr_bibtex_entry_ref_id(
      paste(input_entries[[i]], collapse = "\n"),
      input_keys[[i]],
      link_maps = link_maps,
      prefer_linked_arxiv = TRUE
    )
  }, character(1))
} else {
  character()
}

result_entries <- vector("list", length(input_entries))
converted_arxiv_ids <- character()
doi_of_converted_arxiv_ids <- character()
missing_linked_doi_arxiv_ids <- character()
doi_of_missing_linked_doi_arxiv_ids <- character()
suspicious_arxiv_ids <- character()
resolved_idx <- 0L
for (i in seq_along(input_entries)) {
  input_id <- if (length(input_ids) >= i) as.character(input_ids[[i]]) else input_keys[[i]]
  input_type <- if (length(input_types) >= i) as.character(input_types[[i]]) else NA_character_
  is_arxiv_input <- !is.na(input_id) && grepl("^arxiv:", input_id, ignore.case = TRUE)
  is_article_arxiv <- !is.na(input_type) && identical(input_type, "article") && is_arxiv_input
  input_arxiv_bare <- if (!is.na(input_id) && nzchar(input_id)) {
    litxr:::.litxr_bare_arxiv_id(input_id)
  } else {
    NA_character_
  }
  if (is.na(input_id) || !nzchar(input_id)) {
    result_entries[[i]] <- input_entries[[i]]
    next
  }

  if (length(unresolved_ids) && input_id %in% unresolved_ids) {
    result_entries[[i]] <- input_entries[[i]]
  } else {
    resolved_idx <- resolved_idx + 1L
    if (resolved_idx <= length(output_entries)) {
      result_entries[[i]] <- output_entries[[resolved_idx]]
    } else {
      result_entries[[i]] <- input_entries[[i]]
    }
  }

  linked_doi <- if (!is.null(link_maps) && !is.null(link_maps$arxiv_to_doi) && length(link_maps$arxiv_to_doi)) {
    unname(link_maps$arxiv_to_doi[input_id])
  } else {
    NA_character_
  }
  linked_doi <- if (length(linked_doi) && !is.na(linked_doi[[1L]]) && nzchar(linked_doi[[1L]])) as.character(linked_doi[[1L]]) else NA_character_
  linked_doi_bare <- if (!is.na(linked_doi) && nzchar(linked_doi)) {
    litxr:::.litxr_bare_doi(doi = linked_doi)
  } else {
    NA_character_
  }
  linked_doi_present <- !is.na(linked_doi_bare) && nzchar(linked_doi_bare) && linked_doi_bare %in% available_doi_ids
  linked_doi_known <- !is.na(linked_doi) && nzchar(linked_doi)

  if (is_arxiv_input && linked_doi_present) {
    converted_arxiv_ids <- c(converted_arxiv_ids, input_arxiv_bare)
    doi_of_converted_arxiv_ids <- c(doi_of_converted_arxiv_ids, linked_doi_bare)
  } else if (is_arxiv_input && linked_doi_known && !linked_doi_present) {
    missing_linked_doi_arxiv_ids <- c(missing_linked_doi_arxiv_ids, input_arxiv_bare)
    doi_of_missing_linked_doi_arxiv_ids <- c(doi_of_missing_linked_doi_arxiv_ids, linked_doi_bare)
  } else if (is_article_arxiv && !(input_arxiv_bare %in% identity_arxiv_ids)) {
    suspicious_arxiv_ids <- c(suspicious_arxiv_ids, input_arxiv_bare)
  }
}

written_lines <- unlist(lapply(result_entries, function(entry) c(entry, "")), use.names = FALSE)
writeLines(written_lines, output_bibtex)

emit(list(
  status = "ok",
  converted_arxiv_ids = as.list(unique(converted_arxiv_ids)),
  doi_of_converted_arxiv_ids = as.list(doi_of_converted_arxiv_ids[!duplicated(converted_arxiv_ids)]),
  missing_linked_doi_arxiv_ids = as.list(unique(missing_linked_doi_arxiv_ids)),
  doi_of_missing_linked_doi_arxiv_ids = as.list(doi_of_missing_linked_doi_arxiv_ids[!duplicated(missing_linked_doi_arxiv_ids)]),
  suspicious_arxiv_ids = as.list(unique(suspicious_arxiv_ids))
))
EOF
