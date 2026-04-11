#!/usr/bin/env bash
# compile.sh — compile the CCFN Esri workflow, install dependencies, then patch WSL -> Windows paths
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUTPUT_DIR="$SCRIPT_DIR/ecoscope-workflows-ccfn-smart-download-esri-workflow"
PIXI_TOML="$OUTPUT_DIR/pixi.toml"

# Remove any leftover .pixi env so wt-compiler --clobber can overwrite the directory.
# WSL rm -rf fails on Windows symlinks; use cmd.exe rmdir instead.
echo "Clearing any leftover .pixi env before compile..."
WIN_PIXI=$(wslpath -w "$OUTPUT_DIR/.pixi" 2>/dev/null || true)
if [ -n "$WIN_PIXI" ]; then
  cmd.exe /c rmdir /s /q "$WIN_PIXI" 2>/dev/null || true
fi

echo "Compiling workflow..."
~/.pixi/bin/wt-compiler compile \
    --spec="$SCRIPT_DIR/spec.yaml" \
    --pkg-name-prefix=ecoscope-workflows \
    --results-env-var=ECOSCOPE_WORKFLOWS_RESULTS \
    --no-progress \
    --clobber

# Remove stale pixi.lock so pixi regenerates it with fresh WSL paths.
# A lock file patched to Windows C:/ paths from a prior run would break this install.
echo "Installing pixi environment to generate pixi.lock (WSL path in effect)..."
rm -f "$OUTPUT_DIR/pixi.lock"
~/.pixi/bin/pixi install --manifest-path="$PIXI_TOML"

# Remove the installed env; Desktop reinstalls on import.
# Use cmd.exe to handle Windows symlinks that WSL rm -rf cannot traverse.
echo "Removing .pixi/ env (Desktop will reinstall on import)..."
WIN_PIXI2=$(wslpath -w "$OUTPUT_DIR/.pixi" 2>/dev/null || true)
if [ -n "$WIN_PIXI2" ]; then
  cmd.exe /c rmdir /s /q "$WIN_PIXI2" 2>/dev/null || true
fi
rm -rf "$OUTPUT_DIR/.pixi" 2>/dev/null || true

echo "Patching WSL path -> Windows path in pixi.toml..."
sed -i 's|path = "/mnt/c/|path = "C:/|g' "$PIXI_TOML"

echo "Done. Compiled output: $OUTPUT_DIR"
