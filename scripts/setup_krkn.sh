#!/bin/bash
#
# Simple Krkn setup script
# Creates isolated venv inside krkn directory and installs requirements
#

set -euo pipefail

KRKN_REPO_URL="${KRKN_REPO_URL:-https://github.com/redhat-chaos/krkn.git}"
KRKN_VERSION="${KRKN_VERSION:-}"
WORKSPACE_DIR="${WORKSPACE_DIR:-$(pwd)}"
DATA_DIR="${DATA_DIR:-${WORKSPACE_DIR}/data}"
KRKN_DIR="${DATA_DIR}/krkn"

echo "=========================================="
echo "Krkn Setup"
echo "=========================================="
echo "Repository: ${KRKN_REPO_URL}"
if [ -n "${KRKN_VERSION}" ]; then
    echo "Version: ${KRKN_VERSION}"
else
    echo "Version: latest (default branch)"
fi
echo "Install location: ${KRKN_DIR}"
echo ""

# Clean up old installation
if [ -d "${KRKN_DIR}" ]; then
    echo "Removing old krkn directory..."
    rm -rf "${KRKN_DIR}"
fi

# Create data directory
mkdir -p "${DATA_DIR}"

# Clone krkn
echo "Cloning krkn repository..."
if [ -n "${KRKN_VERSION}" ]; then
    # Clone specific version
    git clone --branch "${KRKN_VERSION}" --single-branch "${KRKN_REPO_URL}" "${KRKN_DIR}"
    echo "✓ Cloned krkn ${KRKN_VERSION}"
else
    # Clone latest from default branch
    git clone "${KRKN_REPO_URL}" "${KRKN_DIR}"
    echo "✓ Cloned krkn (latest)"
fi

# Fix setup.cfg if needed
SETUP_CFG="${KRKN_DIR}/setup.cfg"
if [ -f "${SETUP_CFG}" ] && grep -q "=kraken" "${SETUP_CFG}"; then
    echo "Fixing setup.cfg..."
    sed -i.bak 's/=kraken/=krkn/g' "${SETUP_CFG}"
    rm -f "${SETUP_CFG}.bak"
fi

# Change to krkn directory
cd "${KRKN_DIR}"

# Create virtual environment inside krkn directory
echo ""
echo "Creating virtual environment in ${KRKN_DIR}/venv..."
python3.11 -m venv venv

# Activate venv
source venv/bin/activate

# Upgrade pip
echo "Upgrading pip..."
pip install --upgrade pip

# Install setuptools first (provides pkg_resources)
echo "Installing setuptools..."
pip install  "setuptools<70" wheel

# Install requirements with --no-build-isolation
# This is needed because some packages (ibm_cloud_sdk_core) use pkg_resources
# in their setup.py but also have pyproject.toml
echo ""
echo "Installing krkn requirements..."
echo "This may take 5-10 minutes..."
pip install --no-build-isolation -r requirements.txt

# Verify installation
echo ""
echo "=========================================="
echo "Krkn setup completed successfully!"
echo "=========================================="
echo ""
echo "Installation details:"
echo "  - Krkn directory: ${KRKN_DIR}"
echo "  - Python venv: ${KRKN_DIR}/venv"
echo "  - Run script: ${KRKN_DIR}/run_kraken.py"
echo ""
echo "To run krkn manually:"
echo "  cd ${KRKN_DIR}"
echo "  venv/bin/python run_kraken.py --config <config.yaml>"
echo ""

# Deactivate venv
deactivate

cd "${WORKSPACE_DIR}"
