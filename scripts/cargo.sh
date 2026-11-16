#!/usr/bin/env bash
set -euo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_dir"

# protobuf-build 0.14 rejects protoc versions other than 3.x.
# Its bundled macOS compiler uses x86_64 and runs with Rosetta on Apple Silicon.
if [[ -z "${PROTOC:-}" && "$(uname -s)" == Darwin && "$(uname -m)" == arm64 ]]; then
    if ! protoc --version 2>/dev/null | grep -Eq 'libprotoc 3\.[1-9][0-9]*\.'; then
        bundled_protoc="$(cargo metadata --format-version 1 | python3 -c '
import json, pathlib, sys
packages = json.load(sys.stdin)["packages"]
package = next(p for p in packages if p["name"] == "protobuf-build")
print(pathlib.Path(package["manifest_path"]).parent / "bin" / "protoc-osx-x86_64")')"
        if ! "$bundled_protoc" --version >/dev/null 2>&1; then
            echo 'Install protoc 3.x or set PROTOC to a compatible compiler.' >&2
            exit 1
        fi
        export PROTOC="$bundled_protoc"
    fi
fi

exec cargo "$@"
