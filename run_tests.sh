#!/usr/bin/env bash
set -e
echo "Running Extractor Automated Test Suite..."
pytest -v tests/test_extractor.py
