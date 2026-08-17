#!/bin/sh
set -eu

app_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
app="$app_dir/stockdb.app"

if [ "$(uname -s)" != "Darwin" ]; then
  echo "This repair script must be run on macOS." >&2
  exit 1
fi

if [ ! -d "$app" ] || [ ! -x "$app/Contents/MacOS/stockdb" ]; then
  echo "stockdb.app is missing or incomplete: $app" >&2
  exit 1
fi

# Browser downloads carry quarantine metadata; the local bundle is unsigned.
xattr -dr com.apple.quarantine "$app" 2>/dev/null || true
codesign --force --deep --sign - "$app"

echo "Repaired: $app"
echo "Open it with: open \"$app\""
