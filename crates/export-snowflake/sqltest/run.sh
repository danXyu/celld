#!/bin/sh
# Run the Snowflake SQL against synthetic records on fakesnow (DuckDB).
# Creates a virtualenv under target/ on first use.
set -eu
here=$(cd "$(dirname "$0")" && pwd)
root=$(cd "$here/../../.." && pwd)
venv="$root/target/export-snowflake-sqltest-venv"
if [ ! -x "$venv/bin/pytest" ]; then
    python3 -m venv "$venv"
    "$venv/bin/pip" install -q -r "$here/requirements.txt"
fi
cd "$root"
exec "$venv/bin/pytest" -q "$here" "$@"
