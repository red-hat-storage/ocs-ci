#!/bin/bash
# Wrapper for gitleaks with helpful error messages
# Used by both pre-commit hooks and tox

# Check if gitleaks is installed
if ! command -v gitleaks >/dev/null 2>&1; then
    echo ""
    echo "❌ ERROR: gitleaks is not installed!"
    echo ""
    echo "Gitleaks is required for secret scanning. Please install it:"
    echo ""
    echo "  Mac (Homebrew):"
    echo "    brew install gitleaks"
    echo ""
    echo "  Linux:"
    echo "    Download the latest release from:"
    echo "    https://github.com/gitleaks/gitleaks/releases/latest"
    echo "    Extract and move to /usr/local/bin/"
    echo ""
    exit 1
fi

# Run gitleaks protect (scans staged changes)
if gitleaks protect --verbose --redact --staged; then
    exit 0
else
    echo ""
    echo "❌ SECRETS DETECTED!"
    echo ""
    echo "Gitleaks found potential secrets in your staged changes."
    echo "Check the output above for File and Line number."
    echo ""
    echo "To fix:"
    echo "  1. Review the finding - is it a real secret or test data?"
    echo "  2. If it is test data/placeholder, add this comment to that line:"
    echo "     # gitleaks:allow"
    echo "  3. If it is a REAL secret, remove it and use proper secret management!"
    echo ""
    exit 1
fi
