#!/usr/bin/env bash
# Copyright 2026 OriginGame contributors
# Licensed under the Apache License, Version 2.0.

set -euo pipefail

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)
crate_root=$(cd "$script_dir/.." && pwd -P)

file_list=$(mktemp "${TMPDIR:-/tmp}/sophon-sdk-source-layout.XXXXXX")
sorted_file_list=$(mktemp "${TMPDIR:-/tmp}/sophon-sdk-source-layout.XXXXXX")
trap 'rm -f "$file_list" "$sorted_file_list"' EXIT

cd "$crate_root"

if [[ -f build.rs ]]; then
    printf '%s\n' build.rs >> "$file_list"
fi
for source_root in build src tests examples; do
    if [[ -d "$source_root" ]]; then
        find "$source_root" -type f -name '*.rs' -print >> "$file_list"
    fi
done
LC_ALL=C sort "$file_list" > "$sorted_file_list"

violations=()
while IFS= read -r path; do
    line_count=$(awk 'END { print NR + 0 }' "$path")
    if [[ "$path" == "src/lib.rs" ]]; then
        limit=300
    else
        limit=2000
    fi

    if ((line_count > limit)); then
        violations+=("- $path: $line_count lines (limit $limit)")
    fi
done < "$sorted_file_list"

if ((${#violations[@]} > 0)); then
    printf 'Rust source layout violations:\n' >&2
    printf '%s\n' "${violations[@]}" >&2
    printf 'Split each violating file by responsibility rather than raising the line limit.\n' >&2
    exit 1
fi
