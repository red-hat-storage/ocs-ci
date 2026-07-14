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
    echo "  Fedora:"
    echo "    dnf install gitleaks"
    echo ""
    echo "  Other Linux (download latest release):"
    echo "    GITLEAKS_VERSION=\$(curl -s https://api.github.com/repos/gitleaks/gitleaks/releases/latest | jq -r .tag_name)"
    echo "    GITLEAKS_VERSION_NUM=\${GITLEAKS_VERSION#v}"
    echo "    wget https://github.com/gitleaks/gitleaks/releases/download/\${GITLEAKS_VERSION}/gitleaks_\${GITLEAKS_VERSION_NUM}_linux_x64.tar.gz"
    echo "    tar -xzf gitleaks_\${GITLEAKS_VERSION_NUM}_linux_x64.tar.gz"
    echo "    sudo mv gitleaks /usr/local/bin/"
    echo ""
    exit 1
fi

# Detect if running in CI environment
if [ -n "$CI" ] || [ -n "$GITHUB_ACTIONS" ]; then
    # In CI: scan only the commits in the PR (diff against base branch)
    echo "Running gitleaks in CI mode (scanning PR commits)..."

    # Use GITHUB_BASE_REF to get the target branch (master, release-4.21, etc.)
    BASE_BRANCH="${GITHUB_BASE_REF:-master}"
    echo "Scanning commits: origin/${BASE_BRANCH}..HEAD"

    if gitleaks detect --verbose --redact --log-opts="origin/${BASE_BRANCH}..HEAD"; then
        exit 0
    else
        echo ""
        echo "❌ SECRETS DETECTED!"
        echo ""
        echo "Gitleaks found potential secrets in your PR commits."
        echo "Check the output above for File and Line number."
        echo ""
        echo "To fix:"
        echo "  1. Review the finding - is it a real secret or test data?"
        echo "  2. If it is test data/placeholder, you have two options:"
        echo "     a) Add inline comment to that line: # gitleaks:allow"
        echo "     b) Add the file path to .gitleaks.toml under [allowlist] paths"
        echo "        For example: '''path/to/test/file\\.py''',"
        echo "  3. If it is a REAL secret, remove it and use proper secret management!"
        echo ""
        exit 1
    fi
else
    # Local: scan staged changes (pre-commit hook)
    echo "Running gitleaks in local mode (scanning staged changes)..."
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
        echo "  2. If it is test data/placeholder, you have two options:"
        echo "     a) Add inline comment to that line: # gitleaks:allow"
        echo "     b) Add the file path to .gitleaks.toml under [allowlist] paths"
        echo "        For example: '''path/to/test/file\\.py''',"
        echo "  3. If it is a REAL secret, remove it and use proper secret management!"
        echo ""
        exit 1
    fi
fi
