#!/bin/zsh

set -eu

script_dir="${0:A:h}"
repo_root="${script_dir:h:h}"
source "${script_dir}/_diagnostics.zsh"
diagnose=false

usage() {
  cat <<'EOF'
Usage:
  scripts/human/report_arxiv_category_text_query.sh --inquiry-text TEXT [report_args...]

Purpose:
  Human-readable wrapper around scripts/report_arxiv_category_labels.R for a
  one-off ad-hoc text query.

Notes:
  - This wrapper forces `--output-format md`.
  - All non-help arguments are forwarded to the reporter.
  - `--inquiry-text` is required.
  - `--diagnose` emits step timings and I/O metadata to stderr.
EOF
}

inquiry_text=""
args=()

while (($#)); do
  case "$1" in
    -h|--help)
      usage
      exit 0
      ;;
    --diagnose)
      diagnose=true
      ;;
    --inquiry-text)
      shift
      if (($# == 0)); then
        echo "Missing value for --inquiry-text" >&2
        exit 1
      fi
      inquiry_text="$1"
      ;;
    *)
      args+=("$1")
      ;;
  esac
  shift
done

if [[ -z "$inquiry_text" ]]; then
  echo "Missing required --inquiry-text" >&2
  usage >&2
  exit 1
fi

if $diagnose; then
  diag_log "script=report_arxiv_category_text_query.sh inquiry_bytes=$(printf '%s' "$inquiry_text" | wc -c | tr -d ' ')"
fi

tmp_yaml="$(mktemp "${TMPDIR:-/tmp}/litxr_inquiry_text.XXXXXX.yaml")"
trap 'rm -f "$tmp_yaml"' EXIT

write_started="$(diag_now)"
cat > "$tmp_yaml" <<EOF
ad_hoc_query:
  - |
    ${inquiry_text//$'\n'/$'\n    '}
EOF
if $diagnose; then
  diag_log "step=write_tmp_yaml elapsed_sec=$(diag_elapsed "$write_started" "$(diag_now)")"
  diag_log "input_yaml=$(diag_file_meta "$tmp_yaml")"
fi

report_started="$(diag_now)"
report_md="$(Rscript "$repo_root/scripts/report_arxiv_category_labels.R" \
  --inquiry "$tmp_yaml" \
  "${args[@]}" \
  --output-format md
)"
if $diagnose; then
  diag_log "step=render_category_report elapsed_sec=$(diag_elapsed "$report_started" "$(diag_now)")"
  diag_log "report_bytes=$(printf '%s' "$report_md" | wc -c | tr -d ' ')"
fi

printf '%s\n' "$report_md"
