#!/bin/sh
if ! command -v pwsh >/dev/null 2>&1; then
  echo "pwsh not found; skipping PowerShell tests." >&2
  exit 0
fi
pwsh -File tests/test.ps1
