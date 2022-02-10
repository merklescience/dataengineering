#!/usr/bin/env bash
# This script only exists for the sake of the github action
python -m poetry run interrogate -v --output docs-coverage.txt --fail-under 50 --ignore-init-method --ignore-init-module --ignore-magic --no-color --generate-badge . --badge-format svg
if grep -q PASSED docs-coverage.txt; then
    exit 0
else
    cat docs-coverage.txt
    exit 1
fi
