# AI-Powered Provider/Client Code Review

**🤖 Claude-powered code review** for ocs-ci Provider/Client pattern violations.

Automatically detects missing context managers and markers, providing intelligent suggestions via Claude AI and posting review comments directly to GitHub PRs.

---

## 🎯 Features

✅ **Static Analysis** - AST-based pattern detection
🤖 **Claude AI** - Intelligent, context-aware suggestions
🐙 **GitHub Integration** - Automated PR review comments
⚡ **Fast** - Analyzes only changed lines in PRs
🔧 **Configurable** - Customize patterns and rules

---

## 🚀 Quick Start

### 1. Install Dependencies

```bash
pip install -r tools/code-review/provider-client/requirements.txt
```

### 2. Set API Keys

```bash
# Get from https://console.anthropic.com/
export ANTHROPIC_API_KEY="sk-ant-..."

# Get from https://github.com/settings/tokens
export GITHUB_TOKEN="ghp_..."
```

### 3. Run Locally

```bash
# Basic pattern detection
python tools/code-review/provider-client/review.py

# With Claude AI
python tools/code-review/provider-client/review.py --use-claude

# Review and post to GitHub PR
python tools/code-review/provider-client/review.py --pr 1234 --use-claude --post-to-github
```

### 4. GitHub Actions (Automated)

Already configured! Just add `ANTHROPIC_API_KEY` to repository secrets:

- Go to Settings → Secrets → Actions
- Add secret: `ANTHROPIC_API_KEY`
- Workflow runs automatically on every PR

---

## 📦 Components

### 1. Pattern Analyzer (`analyzers/provider_client_analyzer.py`)

AST-based analyzer that detects:
- Missing `RunWithProviderConfigContextIfAvailable` context managers
- Missing `@runs_on_provider` test markers
- Provider function calls without proper context
- Line-by-line detection with suggestions

### 2. Claude Reviewer (`analyzers/claude_reviewer.py`)

AI-powered reviewer that:
- Sends findings to Claude API
- Gets intelligent, context-aware suggestions
- Formats review comments with code examples
- Explains WHY patterns are needed

### 3. GitHub Integration (`github_integration.py`)

GitHub automation that:
- Posts inline review comments
- Creates summary comments
- Approves PRs when requested
- Handles PR status updates

### 4. Pattern Rules (`analyzers/patterns.yaml`)

Configuration defining:
- Provider-specific functions
- Valid context managers
- Test markers
- Exempted files/paths

### 5. GitHub Actions (`.github/workflows/ai-code-review.yml`)

Automated workflow that:
- Runs on every PR
- Only analyzes Python changes
- Posts Claude-generated reviews
- Never blocks merges (informational only)

---

## 📖 Usage

### Command-Line Options

```bash
python tools/code-review/provider-client/review.py [FILE] [OPTIONS]

Options:
  FILE                  Specific file to analyze
  --diff SPEC          Analyze git diff (e.g., master..HEAD)
  --pr NUMBER          Analyze GitHub PR
  --use-claude         Use Claude AI for intelligent review
  --post-to-github     Post comments to GitHub PR (requires --pr)
  --approve            Approve PR if no errors (with --post-to-github)
  --patterns FILE      Custom patterns.yaml file
```

### Examples

```bash
# Analyze uncommitted changes (basic)
python tools/code-review/provider-client/review.py

# Analyze with AI suggestions
python tools/code-review/provider-client/review.py --use-claude

# Analyze specific file with AI
python tools/code-review/provider-client/review.py tests/my_test.py --use-claude

# Review PR and post comments
python tools/code-review/provider-client/review.py --pr 1234 --use-claude --post-to-github

# Review and approve if clean
python tools/code-review/provider-client/review.py --pr 1234 --use-claude --post-to-github --approve

# Analyze commits
python tools/code-review/provider-client/review.py --diff HEAD~3..HEAD --use-claude
```

---

## 🔧 Configuration

### Customize Detection Patterns

Edit `tools/code-review/provider-client/analyzers/patterns.yaml`:

```yaml
provider_functions:
  - get_provider_address    # Existing
  - your_custom_function    # Add yours

provider_markers:
  - runs_on_provider        # Existing
  - your_custom_marker      # Add yours

exemptions:
  paths:
    - "path/to/exempt.py"   # Exempt specific files
```

### Change Claude Model

Edit `tools/code-review/provider-client/analyzers/claude_reviewer.py`:

```python
self.model = "claude-sonnet-4-5@20250929"  # Default
# Options: claude-opus-4, claude-sonnet-4-5, claude-haiku-4
```

---

## 📚 Documentation

- **[SETUP.md](SETUP.md)** - Complete setup guide with troubleshooting
- **[EXAMPLE.md](EXAMPLE.md)** - Usage examples and patterns
- **[patterns.yaml](analyzers/patterns.yaml)** - Pattern configuration reference

---

## 💡 How It Works

1. **Detect** - Static analyzer finds pattern violations using AST
2. **Review** - Claude analyzes findings and generates suggestions
3. **Post** - GitHub integration posts inline comments on PR
4. **Iterate** - Developers fix issues and re-run review

### Example Output

```
📋 Running pattern analysis...
   Analyzing PR #1234...

✓ No issues found!

🤖 Getting Claude AI review...

📝 Claude Review (1 comment):

1. tests/my_test.py:42
   [ERROR]

   This function calls `get_provider_address()` without proper context.

   **Why this matters:** Provider functions must run in provider cluster
   context to avoid accessing the wrong cluster's resources.

   **Fix:**
   ```python
   with config.RunWithProviderConfigContextIfAvailable():
       address = get_provider_address()
   ```

   Alternatively, add `@runs_on_provider` decorator if the entire test
   requires provider access.

🐙 Posting review to GitHub...
✓ Review posted to PR #1234
```

---

## 🎓 Advanced Usage

### Pre-commit Hook

Add to `.pre-commit-config.yaml`:

```yaml
- repo: local
  hooks:
    - id: provider-client-patterns
      name: Provider/Client Pattern Check
      entry: python tools/code-review/provider-client/review.py
      language: system
      types: [python]
      pass_filenames: false
```

### CI/CD Integration

Make the check **blocking** (fail CI on errors):

Edit `.github/workflows/ai-code-review.yml`:

```yaml
- name: Run AI Code Review
  run: |
    python tools/code-review/provider-client/review.py \
      --pr "${{ github.event.pull_request.number }}" \
      --use-claude \
      --post-to-github

    exit $?  # Fail workflow if errors found
```

---

## 💰 Cost

**Claude API pricing** (Sonnet 4.5):
- ~$3 per million tokens
- Average PR: ~5,000 tokens = $0.015
- 50 PRs/month: ~$0.75/month

---

## 🤝 Contributing

Improvements welcome! To enhance the reviewer:

1. **Add patterns** - Update `patterns.yaml`
2. **Improve prompts** - Edit `claude_reviewer.py`
3. **Extend analysis** - Enhance `provider_client_analyzer.py`
4. **Test** - Run on real PRs before committing

---

## 📄 Files Structure

```
tools/code-review/provider-client/
├── analyzers/
│   ├── provider_client_analyzer.py  # Core AST analyzer
│   ├── claude_reviewer.py           # Claude AI integration
│   └── patterns.yaml                # Detection rules
├── github_integration.py            # GitHub PR comments
├── ai_review_poc.py                 # Main CLI tool
├── requirements-ai-review.txt       # Python dependencies
├── .env.example                     # Environment template
├── README.md                        # This file
├── SETUP.md                         # Setup guide
└── EXAMPLE.md                       # Usage examples

.github/workflows/
└── ai-code-review.yml              # GitHub Actions workflow
```

---

## 🐛 Troubleshooting

See **[SETUP.md](SETUP.md)** for detailed troubleshooting.

**Common issues:**

- **"anthropic not installed"** → `pip install anthropic`
- **"API key not set"** → `export ANTHROPIC_API_KEY="..."`
- **"gh not found"** → Install GitHub CLI from https://cli.github.com/

---

## ⚡ Requirements

- Python 3.10+
- PyYAML
- anthropic (Claude SDK)
- Git
- GitHub CLI (gh) - for PR posting

---

**Built with ❤️ using [Claude Code](https://claude.ai/code)**
