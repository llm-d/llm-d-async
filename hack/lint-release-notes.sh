#!/usr/bin/env bash
# Validate release-note fragments under release-notes.d/unreleased/.
#
# Each fragment must be YAML front matter with the keys pr, url, author, and
# date, followed by a non-empty note body. Exits non-zero (listing every
# problem) if any fragment is malformed. Run via `make release-notes-lint`.
set -euo pipefail

DIR="release-notes.d/unreleased"
shopt -s nullglob
fragments=("$DIR"/*.md)

if [[ ${#fragments[@]} -eq 0 ]]; then
  echo "No release-note fragments to validate."
  exit 0
fi

rc=0
for f in "${fragments[@]}"; do
  # Front matter is the block between the first two '---' lines.
  if [[ "$(sed -n '1p' "$f")" != "---" ]]; then
    echo "ERROR: $f: must start with '---' front matter" >&2
    rc=1
    continue
  fi
  for key in pr url author date; do
    if ! sed -n '2,/^---$/p' "$f" | grep -qE "^${key}: *[^ ]"; then
      echo "ERROR: $f: missing or empty front-matter key '${key}'" >&2
      rc=1
    fi
  done
  # Body is everything after the second '---'; must be non-empty.
  body="$(awk 'p {print; next} /^---$/ {if (++c == 2) p = 1}' "$f" | tr -d '[:space:]')"
  if [[ -z "$body" ]]; then
    echo "ERROR: $f: note body is empty" >&2
    rc=1
  fi
done

if [[ $rc -eq 0 ]]; then
  echo "All ${#fragments[@]} release-note fragment(s) are well-formed."
fi
exit $rc
