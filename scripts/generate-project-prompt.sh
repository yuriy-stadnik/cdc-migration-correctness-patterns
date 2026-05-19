#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  ./scripts/generate-project-prompt.sh [options]

Options:
  -o, --output <path>       Output file path (default: PROJECT_PROMPT_BUNDLE.md)
  --include-untracked       Include untracked files (excluding .gitignored)
  --max-file-bytes <bytes>  Skip text files larger than this size (default: 1000000)
  -h, --help                Show this help
EOF
}

to_abs_path() {
  local input="$1"
  if [[ "$input" = /* ]]; then
    printf '%s\n' "$input"
  else
    printf '%s/%s\n' "$PWD" "$input"
  fi
}

description_for_file() {
  local file="$1"
  case "$file" in
    README.md) echo "Top-level project documentation and usage guide." ;;
    local/README.md) echo "Local deployment documentation and verification workflow." ;;
    *.tf) echo "Terraform infrastructure definition." ;;
    *.tfvars|*.tfvars.json) echo "Terraform variable values." ;;
    *.sql) echo "SQL schema, initialization, or transformation script." ;;
    *docker-compose*.yml) echo "Docker Compose orchestration configuration." ;;
    Dockerfile) echo "Container image build recipe." ;;
    *.properties) echo "Service/runtime properties configuration." ;;
    *.json)
      if [[ "$file" == *connector* ]]; then
        echo "CDC connector configuration."
      else
        echo "JSON configuration or data file."
      fi
      ;;
    *.md) echo "Project documentation file." ;;
    *.sh) echo "Shell automation script." ;;
    *.bat) echo "Windows batch automation script." ;;
    *.tpl) echo "Template file used to generate runtime scripts/configuration." ;;
    *.java) echo "Java source file for Lambda or local stream consumer logic." ;;
    gradlew|gradlew.bat) echo "Gradle wrapper launcher script." ;;
    build.gradle|settings.gradle) echo "Gradle build configuration." ;;
    *) echo "Project file." ;;
  esac
}

lang_for_file() {
  local file="$1"
  case "$file" in
    *.tf) echo "hcl" ;;
    *.sql) echo "sql" ;;
    *.yml|*.yaml) echo "yaml" ;;
    *.json) echo "json" ;;
    *.md) echo "markdown" ;;
    *.sh|*.tpl) echo "bash" ;;
    *.bat) echo "bat" ;;
    *.java) echo "java" ;;
    *.properties) echo "properties" ;;
    *) echo "text" ;;
  esac
}

is_text_file() {
  local file="$1"
  grep -Iq . "$file"
}

ROOT_DIR="$(git rev-parse --show-toplevel 2>/dev/null || pwd)"
cd "$ROOT_DIR"

OUTPUT_FILE="PROJECT_PROMPT_BUNDLE.md"
INCLUDE_UNTRACKED=0
MAX_FILE_BYTES=1000000

while (($# > 0)); do
  case "$1" in
    -o|--output)
      shift
      OUTPUT_FILE="${1:-}"
      if [[ -z "$OUTPUT_FILE" ]]; then
        echo "Missing value for --output" >&2
        exit 1
      fi
      ;;
    --include-untracked)
      INCLUDE_UNTRACKED=1
      ;;
    --max-file-bytes)
      shift
      MAX_FILE_BYTES="${1:-}"
      if [[ -z "$MAX_FILE_BYTES" ]]; then
        echo "Missing value for --max-file-bytes" >&2
        exit 1
      fi
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
  shift
done

OUTPUT_ABS="$(to_abs_path "$OUTPUT_FILE")"

declare -a files
declare -a skipped

while IFS= read -r file; do
  [[ -z "$file" ]] && continue
  [[ -f "$file" ]] || continue
  files+=("$file")
done < <(git ls-files --cached)

if [[ "$INCLUDE_UNTRACKED" -eq 1 ]]; then
  while IFS= read -r file; do
    [[ -z "$file" ]] && continue
    [[ -f "$file" ]] || continue
    files+=("$file")
  done < <(git ls-files --others --exclude-standard)
fi

IFS=$'\n' files=($(printf "%s\n" "${files[@]}" | sort -u))
unset IFS

{
  echo "# Project Prompt Bundle"
  echo
  echo "Generated: $(date -u '+%Y-%m-%dT%H:%M:%SZ')"
  echo "Root: <repo-root>"
  echo "File count scanned: ${#files[@]}"
  echo
  echo "This bundle is intended as context prompt input for another LLM."
  echo
} > "$OUTPUT_ABS"

included_count=0

for file in "${files[@]}"; do
  FILE_ABS="$(to_abs_path "$file")"
  [[ "$FILE_ABS" == "$OUTPUT_ABS" ]] && continue

  size_bytes="$(wc -c < "$file" | tr -d '[:space:]')"
  desc="$(description_for_file "$file")"

  if ! is_text_file "$file"; then
    skipped+=("$file|binary|$size_bytes|$desc")
    continue
  fi

  if (( size_bytes > MAX_FILE_BYTES )); then
    skipped+=("$file|too_large|$size_bytes|$desc")
    continue
  fi

  line_count="$(wc -l < "$file" | tr -d '[:space:]')"
  lang="$(lang_for_file "$file")"

  {
    echo "## $file"
    echo
    echo "- Description: $desc"
    echo "- Size bytes: $size_bytes"
    echo "- Line count: $line_count"
    echo
    echo "\`\`\`\`$lang"
    cat "$file"
    echo
    echo "\`\`\`\`"
    echo
  } >> "$OUTPUT_ABS"

  included_count=$((included_count + 1))
done

{
  echo "## Skipped Files"
  echo
  echo "- Included files: $included_count"
  echo "- Skipped files: ${#skipped[@]}"
  echo
  for entry in "${skipped[@]}"; do
    file="${entry%%|*}"
    rest="${entry#*|}"
    reason="${rest%%|*}"
    rest2="${rest#*|}"
    size="${rest2%%|*}"
    desc="${rest2#*|}"
    echo "- $file | reason=$reason | size_bytes=$size | description=$desc"
  done
} >> "$OUTPUT_ABS"

echo "Prompt bundle generated: $OUTPUT_ABS"
