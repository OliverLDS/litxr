#!/bin/zsh

diag_now() {
  perl -MTime::HiRes=time -e 'printf "%.6f\n", time'
}

diag_elapsed() {
  perl -MTime::HiRes=time -e 'printf "%.3f\n", $ARGV[0] - $ARGV[1]' "$2" "$1"
}

diag_file_meta() {
  local path="$1"
  if [[ -z "$path" ]]; then
    print -r -- "path=[missing] exists=false size_bytes=NA mtime=[missing]"
    return 0
  fi

  if [[ -e "$path" ]]; then
    local size mtime
    size="$(stat -f%z "$path" 2>/dev/null || print -r -- NA)"
    mtime="$(stat -f%Sm -t "%Y-%m-%d %H:%M:%S %Z" "$path" 2>/dev/null || print -r -- "[missing]")"
    print -r -- "path=${path} exists=true size_bytes=${size} mtime=${mtime}"
  else
    print -r -- "path=${path} exists=false size_bytes=NA mtime=[missing]"
  fi
}

diag_log() {
  print -u2 -- "diagnostics: $*"
}
