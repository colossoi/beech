#!/bin/sh
set -eu

compiler=${1:-thrift}
if [ "$("$compiler" -version | tr -d '\r')" != 'Thrift version 0.24.0' ]; then
    echo 'Use Apache Thrift 0.24.0' >&2
    exit 1
fi

script_directory=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
output_directory="$script_directory/../src/codec/generated"
"$compiler" --gen rs -out "$output_directory" "$script_directory/beech.thrift"
output_path="$output_directory/beech.rs"

# Thrift 0.24.0 incorrectly boxes elements when reading list<union>, although
# the declared Vec element type is unboxed. Keep this workaround reproducible.
count=$(awk '{ n += gsub(/val\.push\(Box::new\(elem\)\)/, "") } END { print n + 0 }' "$output_path")
if [ "$count" -ne 1 ]; then
    echo 'Expected exactly one list<Scalar> generator defect; review the output' >&2
    exit 1
fi

# No services are defined; this unconditional generated import needs the unused
# Thrift server feature. The old rustfmt attribute is unsupported by modern Rust.
temporary=$(mktemp "$output_path.XXXXXX")
trap 'rm -f "$temporary"' 0
tr -d '\r' < "$output_path" | sed \
    -e 's/val\.push(Box::new(elem))/val.push(elem)/g' \
    -e '/^use thrift::server::TProcessor;$/d' \
    -e '/^#!\[cfg_attr(rustfmt, rustfmt_skip)\]$/d' > "$temporary"
cat "$temporary" > "$output_path"
rustfmt --edition 2024 "$output_path"
