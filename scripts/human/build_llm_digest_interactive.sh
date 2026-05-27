#!/bin/zsh

set -eu

script_dir="${0:A:h}"
repo_root="${script_dir:A:h:h}"
script_root="${repo_root}/scripts"
json_path_default="~/Downloads/litxr_schema.json"
prompt_version_default="v4.0"
return_format_default="download_json_file"

usage() {
  cat <<'EOF'
Usage:
  scripts/human/build_llm_digest_interactive.sh --ref-id REF_ID [--json_path ~/Downloads/litxr_schema.json] [--prompt-version v4.0] [--return-format download_json_file|inline_raw_json]

Options:
  --ref-id REF_ID       Canonical litxr ref_id to build or revise.
                        Bare arXiv ids like 2510.22085 are also accepted.
  --json_path PATH      Downloaded JSON path to ingest.
                        Default: ~/Downloads/litxr_schema.json
  --prompt-version V    Prompt template version metadata to include.
                        Default: v4.0
  --return-format MODE  Prompt return mode for the upstream prompt.
                        Default asks for a downloadable JSON file.
                        `inline_raw_json` asks for raw JSON to be pasted from
                        the clipboard at ingest time.
  -h, --help            Show this help message.

Workflow:
  1. Check whether the ref_id already has a digest.
  2. Build a create or revise prompt and copy it with pbcopy.
  3. Download litxr_schema.json locally and confirm ingest.
  4. Run scripts/ingest_llm_digest_json.R.
EOF
}

ref_id=""
json_path="$json_path_default"
prompt_version="$prompt_version_default"
return_format="$return_format_default"

while [[ $# -gt 0 ]]; do
  case "$1" in
    -h|--help)
      usage
      exit 0
      ;;
    --ref-id)
      if [[ $# -lt 2 ]]; then
        print -u2 "Missing value for --ref-id"
        exit 1
      fi
      ref_id="$2"
      shift 2
      ;;
    --json_path)
      if [[ $# -lt 2 ]]; then
        print -u2 "Missing value for --json_path"
        exit 1
      fi
      json_path="$2"
      shift 2
      ;;
    --prompt-version)
      if [[ $# -lt 2 ]]; then
        print -u2 "Missing value for --prompt-version"
        exit 1
      fi
      prompt_version="$2"
      shift 2
      ;;
    --return-format)
      if [[ $# -lt 2 ]]; then
        print -u2 "Missing value for --return-format"
        exit 1
      fi
      return_format="$2"
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

if [[ -z "$ref_id" ]]; then
  usage
  print -u2 "Missing --ref-id"
  exit 1
fi

if [[ "$return_format" != "download_json_file" && "$return_format" != "inline_raw_json" ]]; then
  usage
  print -u2 "Missing or invalid --return-format: $return_format"
  exit 1
fi

normalize_ref_id() {
  local value="$1"
  if [[ "$value" =~ ^[0-9]{4}\.[0-9]{4,5}(v[0-9]+)?$ ]]; then
    print -r -- "arxiv:${value}"
  else
    print -r -- "$value"
  fi
}

ref_id="$(normalize_ref_id "$ref_id")"

status_json="$(Rscript "$script_root/check_llm_digest_status.R" --ref-id "$ref_id")"
if [[ "$(Rscript -e 'x<-jsonlite::fromJSON(commandArgs(trailingOnly=TRUE)[1]); if (!identical(x$status, "ok")) quit(save="no", status=1L); cat(if (isTRUE(x$already_digested)) "yes" else "no")' "$status_json")" == "yes" ]]; then
  print -u2 "Warning: $ref_id already has a digest; the workflow will continue in revise mode."
fi
mode="$(Rscript -e 'x<-jsonlite::fromJSON(commandArgs(trailingOnly=TRUE)[1]); if (!identical(x$status, "ok")) quit(save="no", status=1L); cat(if (isTRUE(x$already_digested)) "revise" else "create")' "$status_json")"

prompt_json="$(Rscript "$script_root/build_llm_digest_prompt.R" --ref-id "$ref_id" --mode "$mode" --prompt-version "$prompt_version" --return-format "$return_format")"
prompt_text="$(Rscript -e 'x<-jsonlite::fromJSON(commandArgs(trailingOnly=TRUE)[1]); if (!identical(x$status, "ok")) quit(save="no", status=1L); cat(x$prompt)' "$prompt_json")"

printf '%s' "$prompt_text" | pbcopy
print "Prompt copied to clipboard with pbcopy."
if [[ "$return_format" == "inline_raw_json" ]]; then
  print "After copying the raw JSON from chat into the clipboard:"
  print -r -- "  it will be ingested directly from pbpaste"
else
  print "After downloading the returned file to:"
  json_path_display="${json_path/#\~/$HOME}"
  print -r -- "  ${json_path_display}"
fi
print ""
printf '%s' "Type Y to ingest now, or N to stop: "
read -r answer
answer="${answer:u}"

if [[ "$answer" != "Y" ]]; then
  print "Stopped without ingesting a digest."
  exit 0
fi

ingest_args=(
  "$script_root/ingest_llm_digest_json.R"
  --ref-id "$ref_id"
  --mode "$mode"
  --prompt-version "$prompt_version"
)

if [[ "$return_format" == "inline_raw_json" ]]; then
  json_raw="$(pbpaste)"
  if [[ -z "$json_raw" ]]; then
    print -u2 "Clipboard is empty. Copy the raw JSON from chat before ingesting."
    exit 1
  fi
  ingest_args+=( --json-raw "$json_raw" )
else
  ingest_args+=( --json-path "$json_path" )
fi

Rscript "${ingest_args[@]}"
