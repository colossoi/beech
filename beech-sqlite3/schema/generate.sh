#!/bin/sh
set -eu

compiler=${1:-thrift}
if [ "$("$compiler" -version | tr -d '\r')" != 'Thrift version 0.24.0' ]; then
    echo 'Use Apache Thrift 0.24.0' >&2
    exit 1
fi

script_directory=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
output_directory="$script_directory/../src/generated"
"$compiler" --gen rs -out "$output_directory" "$script_directory/plan.thrift"
output_path="$output_directory/plan.rs"

# No services are defined; this unconditional generated import needs the unused
# Thrift server feature. The old rustfmt attribute is unsupported by modern Rust.
temporary=$(mktemp "$output_path.XXXXXX")
trap 'rm -f "$temporary"' 0
tr -d '\r' < "$output_path" | sed \
    -e '/^use thrift::server::TProcessor;$/d' \
    -e '/^#!\[cfg_attr(rustfmt, rustfmt_skip)\]$/d' \
    -e 's/thrift::/beech_core::thrift::/g' > "$temporary"
cat "$temporary" > "$output_path"
rustfmt --edition 2024 "$output_path"
