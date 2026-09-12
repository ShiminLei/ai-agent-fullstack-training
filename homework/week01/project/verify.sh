#!/usr/bin/env sh
set -eu

python -m pytest -q
python -m ruff check .
