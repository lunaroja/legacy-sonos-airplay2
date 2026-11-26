#!/bin/sh
# Source and output remain local; compilation has no network access.
set -eu
recipe=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
[ "$#" -eq 1 ] || { echo "Usage: $0 EMPTY_OUTPUT_DIRECTORY" >&2; exit 2; }
mkdir -p -- "$1"
output=$(CDPATH='' cd -- "$1" && pwd)
[ -z "$(ls -A "$output")" ] || { echo 'Output directory must be empty' >&2; exit 2; }
python3 -B "$recipe/fetch-source.py"
docker build --platform linux/arm64 --target toolchain --iidfile "$recipe/.cache/toolchain.id" "$recipe"
toolchain=$(cat "$recipe/.cache/toolchain.id")
docker run --rm --platform linux/arm64 --network none --read-only \
  --cap-drop ALL --security-opt no-new-privileges --memory 2g --cpus 2 --pids-limit 256 \
  --mount "type=bind,src=$recipe,dst=/recipe,readonly" \
  --mount "type=bind,src=$output,dst=/out" \
  --tmpfs /build:rw,exec,size=1g,mode=0755 --tmpfs /tmp:rw,exec,size=128m,mode=1777 \
  --env "RECIPE_TOOLCHAIN_IMAGE=$toolchain" \
  "$toolchain" python3 -B /recipe/build.py
