#!/usr/bin/env bash
# Assemble per-PR fragments from release-notes.d/unreleased/ into a new release
# section at the top of RELEASE-NOTES.md, then remove the consumed fragments.
#
# Usage: hack/assemble-release-notes.sh <version> [date]
#   version  release tag, e.g. v0.8.0 (required)
#   date     release date YYYY-MM-DD (default: today, UTC)
#
# Typically invoked as `make release-notes VERSION=v0.8.0` while preparing a
# release. The result is left staged in the working tree for you to review and
# commit; this script performs no git operations.
set -euo pipefail

VERSION="${1:-}"
RELEASE_DATE="${2:-$(date -u +%F)}"
if [[ -z "$VERSION" ]]; then
  echo "usage: $0 <version> [date]" >&2
  exit 2
fi

DIR="release-notes.d/unreleased"
NOTES="RELEASE-NOTES.md"
MARKER="<!-- BEGIN RELEASES -->"

# Reuse the linter so malformed fragments abort the release rather than
# silently producing a broken section.
hack/lint-release-notes.sh

shopt -s nullglob
fragments=("$DIR"/*.md)
if [[ ${#fragments[@]} -eq 0 ]]; then
  echo "No fragments in $DIR; nothing to assemble."
  exit 0
fi
if [[ ! -f "$NOTES" ]] || ! grep -qF "$MARKER" "$NOTES"; then
  echo "ERROR: $NOTES is missing or has no '$MARKER' marker" >&2
  exit 1
fi

# Collect "<date>\t<pr>\t<url>\t<note>" rows, sorted by date then PR number.
rows=""; section=""; new=""
trap 'rm -f "$rows" "$section" "$new"' EXIT
rows="$(mktemp)"
for f in "${fragments[@]}"; do
  pr="$(sed -n 's/^pr: *//p' "$f" | head -1)"
  url="$(sed -n 's/^url: *//p' "$f" | head -1)"
  date="$(sed -n 's/^date: *//p' "$f" | head -1)"
  note="$(awk 'p {print; next} /^---$/ {if (++c == 2) p = 1}' "$f" \
    | sed '/^[[:space:]]*$/d' | tr '\n' ' ' | sed 's/[[:space:]]\+/ /g; s/^ //; s/ $//')"
  printf '%s\t%s\t%s\t%s\n' "$date" "$pr" "$url" "$note" >> "$rows"
done
sort -k1,1 -k2,2n -o "$rows" "$rows"

section="$(mktemp)"
{
  printf 'RELEASE %s %s\n' "$VERSION" "$RELEASE_DATE"
  while IFS=$'\t' read -r date pr url note; do
    printf '%s %s %s\n' "$date" "$url" "$note"
  done < "$rows"
  printf '\n'
} > "$section"

# Insert the new section immediately after the marker line.
new="$(mktemp)"
awk -v marker="$MARKER" -v sectionfile="$section" '
  { print }
  $0 == marker && !done {
    print ""
    while ((getline line < sectionfile) > 0) print line
    done = 1
  }
' "$NOTES" > "$new"
mv "$new" "$NOTES"

git rm --quiet -- "${fragments[@]}" 2>/dev/null || rm -f "${fragments[@]}"

echo "Assembled ${#fragments[@]} fragment(s) into $NOTES under 'RELEASE $VERSION $RELEASE_DATE'."
echo "Review the changes and commit them as part of the release."
