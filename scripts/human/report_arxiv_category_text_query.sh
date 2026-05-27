#!/bin/zsh

set -eu

script_dir="${0:A:h}"
repo_root="${script_dir:h:h}"

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

tmp_yaml="$(mktemp "${TMPDIR:-/tmp}/litxr_inquiry_text.XXXXXX.yaml")"
trap 'rm -f "$tmp_yaml"' EXIT

cat > "$tmp_yaml" <<EOF
ad_hoc_query:
  - |
    ${inquiry_text//$'\n'/$'\n    '}
EOF

Rscript "$repo_root/scripts/report_arxiv_category_labels.R" \
  --inquiry "$tmp_yaml" \
  "${args[@]}" \
  --output-format md
