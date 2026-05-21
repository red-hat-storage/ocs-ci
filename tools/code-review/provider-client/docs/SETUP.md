# AI Code Review Setup Guide

Complete guide to setting up Claude-powered Provider/Client pattern reviews.

## 🎯 What This Does

Automatically reviews PRs for Provider/Client pattern violations using:
1. **Static Analysis** - AST-based pattern detection
2. **Claude AI** - Intelligent, context-aware suggestions
3. **GitHub Integration** - Automated PR review comments

## 📋 Prerequisites

### 1. Install Dependencies

```bash
pip install -r tools/code-review/provider-client/requirements.txt
```

This installs:
- `anthropic` - Claude API client
- `pyyaml` - Configuration parsing

### 2. Install GitHub CLI

**Linux:**
```bash
# Debian/Ubuntu
curl -fsSL https://cli.github.com/packages/githubcli-archive-keyring.gpg | sudo dd of=/usr/share/keyrings/githubcli-archive-keyring.gpg
echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/githubcli-archive-keyring.gpg] https://cli.github.com/packages stable main" | sudo tee /etc/apt/sources.list.d/github-cli.list > /dev/null
sudo apt update
sudo apt install gh

# Fedora
sudo dnf install gh
```

**macOS:**
```bash
brew install gh
```

**Windows:**
```bash
winget install --id GitHub.cli
```

### 3. Get API Keys

#### Anthropic API Key (for Claude)

1. Go to https://console.anthropic.com/
2. Sign up or log in
3. Navigate to "API Keys"
4. Create a new API key
5. Copy the key (starts with `sk-ant-`)

#### GitHub Token (for PR comments)

1. Go to https://github.com/settings/tokens
2. Click "Generate new token (classic)"
3. Select scopes:
   - `repo` (for private repos) or
   - `public_repo` (for public repos only)
4. Generate and copy the token

### 4. Configure Environment Variables

Create `tools/code-review/provider-client/.env` file (copy from template):

```bash
cp tools/code-review/provider-client/.env.example tools/code-review/provider-client/.env
```

Edit `tools/code-review/provider-client/.env`:

```bash
ANTHROPIC_API_KEY=sk-ant-api03-your-key-here
GITHUB_TOKEN=ghp_your_token_here
```

**⚠️ Security:** The `.env` file is in `.gitignore` - never commit it!

Alternatively, export variables in your shell:

```bash
export ANTHROPIC_API_KEY="sk-ant-api03-..."
export GITHUB_TOKEN="ghp_..."
```

## 🚀 Usage

### Local Testing

#### Basic Pattern Detection (No AI)

```bash
# Analyze current uncommitted changes
python tools/code-review/provider-client/review.py

# Analyze specific file
python tools/code-review/provider-client/review.py tests/my_test.py

# Analyze commits
python tools/code-review/provider-client/review.py --diff master..HEAD
```

#### With Claude AI Review

```bash
# Get intelligent suggestions from Claude
python tools/code-review/provider-client/review.py --use-claude

# Analyze specific file with AI
python tools/code-review/provider-client/review.py tests/my_test.py --use-claude

# Analyze PR locally (no GitHub posting)
python tools/code-review/provider-client/review.py --pr 1234 --use-claude
```

#### Post to GitHub PR

```bash
# Analyze and post review comments to PR
python tools/code-review/provider-client/review.py --pr 1234 --use-claude --post-to-github

# Approve PR if no errors found
python tools/code-review/provider-client/review.py --pr 1234 --use-claude --post-to-github --approve
```

### GitHub Actions (Automated)

The workflow runs automatically on every PR that modifies Python files.

#### Setup for Repository

1. **Add Repository Secret:**
   - Go to repository Settings → Secrets and variables → Actions
   - Click "New repository secret"
   - Name: `ANTHROPIC_API_KEY`
   - Value: Your Claude API key
   - Save

2. **Verify Workflow:**
   - Open a PR with Python changes
   - Check the "Actions" tab
   - Look for "AI Code Review - Provider/Client Patterns" workflow

3. **Review Comments:**
   - Comments appear inline on changed code
   - Summary posted as PR comment
   - Errors require fixes before merge

#### Workflow Configuration

Located at: `.github/workflows/ai-code-review.yml`

**Default behavior:**
- Runs on: PR opened, synchronized, reopened
- Only when: Python files changed
- Posts: Inline comments and summary
- Never fails: PR checks (informational only)

**To make it blocking (fail CI on errors):**

Edit `.github/workflows/ai-code-review.yml`:

```yaml
- name: Run AI Code Review
  run: |
    python tools/code-review/provider-client/review.py \
      --pr "${PR_NUMBER}" \
      --use-claude \
      --post-to-github

    # Exit with analyzer's exit code (fail on errors)
    exit $?
```

## 🔧 Configuration

### Pattern Rules

Edit `tools/code-review/provider-client/analyzers/patterns.yaml` to customize detection:

```yaml
# Add your provider functions
provider_functions:
  - get_provider_address
  - get_provider_info
  - your_custom_function  # Add here

# Add your markers
provider_markers:
  - runs_on_provider
  - your_custom_marker  # Add here

# Exempt files
exemptions:
  paths:
    - "path/to/exempt_file.py"
```

### Claude Model

Edit `tools/code-review/provider-client/analyzers/claude_reviewer.py`:

```python
self.model = "claude-sonnet-4-5@20250929"  # Change model here
```

Available models:
- `claude-sonnet-4-5@20250929` - Fast, capable (default)
- `claude-opus-4@20250514` - Most capable
- `claude-haiku-4@20250313` - Fastest, cheapest

## 📊 Cost Estimation

**Claude API pricing** (as of 2025):
- Sonnet 4.5: ~$3 per million input tokens
- Average PR review: ~5,000 tokens = $0.015

**For a typical repository:**
- 50 PRs/month
- Cost: ~$0.75/month

## 🐛 Troubleshooting

### "anthropic package not installed"

```bash
pip install anthropic
```

### "ANTHROPIC_API_KEY environment variable not set"

```bash
export ANTHROPIC_API_KEY="your-key-here"
# Or add to tools/code-review/provider-client/.env file
```

### "GitHub CLI (gh) not found"

Install GitHub CLI: https://cli.github.com/

Then authenticate:
```bash
gh auth login
```

### "Could not find PR #1234"

Ensure:
1. PR exists and is open
2. You have access to the repository
3. `GITHUB_TOKEN` has correct permissions

### Workflow not running

Check:
1. Workflow file exists: `.github/workflows/ai-code-review.yml`
2. `ANTHROPIC_API_KEY` secret is set in repository settings
3. PR modifies `.py` files (workflow only runs on Python changes)

### Claude API errors

- **Rate limit:** Wait or upgrade API plan
- **Invalid key:** Regenerate at https://console.anthropic.com/
- **Timeout:** Retry or reduce analyzed code

## 📚 Examples

### Example 1: Fix Missing Context Manager

**Before (detected by analyzer):**
```python
def test_provider():
    address = get_provider_address()  # ❌ Missing context
```

**Claude suggests:**
```python
def test_provider():
    with config.RunWithProviderConfigContextIfAvailable():
        address = get_provider_address()  # ✅ Correct
```

### Example 2: Add Test Marker

**Before:**
```python
def test_provider_health():
    check_provider_health()  # ❌ Missing marker
```

**Claude suggests:**
```python
@runs_on_provider
def test_provider_health():
    check_provider_health()  # ✅ Correct
```

## 🎓 Next Steps

1. **Test locally** with `--use-claude` on your branch
2. **Open a PR** to see automated reviews
3. **Customize patterns** in `patterns.yaml` for your needs
4. **Monitor costs** in Anthropic console
5. **Iterate** on review prompts in `claude_reviewer.py`

## 🤝 Contributing

To improve the AI reviewer:

1. **Add patterns** - Update `patterns.yaml`
2. **Improve prompts** - Edit `_build_review_prompt()` in `claude_reviewer.py`
3. **Enhance analysis** - Extend `ProviderPatternVisitor` in `provider_client_analyzer.py`
4. **Test changes** - Run on real PRs before committing

## 📄 License

Part of ocs-ci project. See main repository LICENSE.
