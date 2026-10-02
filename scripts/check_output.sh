#!/usr/bin/env bash
# Verifies that the consumer produced an output log with the expected
# number of processed-event lines and a summary section.
#
# Usage: scripts/check_output.sh [OUTPUT_FILE] [EXPECTED_EVENTS]
set -euo pipefail

OUTPUT_FILE="${1:-output/orders.log}"
EXPECTED="${2:-20}"

if [ ! -f "$OUTPUT_FILE" ]; then
  echo "FAIL: $OUTPUT_FILE does not exist"
  exit 1
fi

event_lines="$(grep -c '^order=' "$OUTPUT_FILE" || true)"
echo "Found ${event_lines} processed-event lines in ${OUTPUT_FILE} (expected ${EXPECTED})"

if [ "$event_lines" -lt "$EXPECTED" ]; then
  echo "FAIL: expected at least ${EXPECTED} processed events, got ${event_lines}"
  exit 1
fi

if ! grep -q '^--- summary ---$' "$OUTPUT_FILE"; then
  echo "FAIL: no summary section found in ${OUTPUT_FILE}"
  exit 1
fi

echo "OK: output file has ${event_lines} events and a summary"
tail -n 10 "$OUTPUT_FILE"
