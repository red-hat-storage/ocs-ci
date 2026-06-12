# Quickstart Guide - AI Code Review

Get started with Claude-powered Provider/Client pattern reviews in **5 minutes**.

---

## ⚡ 5-Minute Setup

### 1. Install Dependencies (1 min)

```bash
cd /path/to/ocs-ci
pip install -r tools/code-review/provider-client/requirements.txt
```

### 2. Get API Key (2 min)

**Visit:** https://console.anthropic.com/

1. Sign up or log in
2. Go to "API Keys"
3. Create new key
4. Copy the key (starts with `sk-ant-`)

### 3. Configure Environment (1 min)

```bash
# Create .env file
cp tools/code-review/provider-client/.env.example tools/code-review/provider-client/.env

# Edit and add your key
nano tools/code-review/provider-client/.env

# Or export directly
export ANTHROPIC_API_KEY="sk-ant-api03-your-key-here"
```

### 4. Test It (1 min)

```bash
# Run on current changes
python tools/code-review/provider-client/review.py --use-claude

# Or test on a file
python tools/code-review/provider-client/review.py tests/some_test.py --use-claude
```

**Done! ✅** You now have AI-powered code review.

---

## 🚀 Common Use Cases

### Use Case 1: Review Before Commit

```bash
# Make changes to test files
vim tests/functional/my_test.py

# Run AI review
python tools/code-review/provider-client/review.py --use-claude

# Fix any issues Claude finds
# Commit when clean
git commit -s -m "Add provider tests"
```

### Use Case 2: Review a PR

```bash
# Get intelligent review of PR changes
python tools/code-review/provider-client/review.py --pr 1234 --use-claude

# Post review comments to GitHub
export GITHUB_TOKEN="ghp_your_token"
python tools/code-review/provider-client/review.py --pr 1234 --use-claude --post-to-github
```

### Use Case 3: Automated GitHub Actions

**Already set up!** Just add API key to repository secrets:

1. Go to: `Settings → Secrets → Actions`
2. Add: `ANTHROPIC_API_KEY`
3. Value: Your Claude API key
4. Save

**That's it!** Every PR now gets automatic AI review.

---

## 📝 Quick Command Reference

```bash
# Basic pattern check (no AI)
python tools/code-review/provider-client/review.py

# With Claude AI
python tools/code-review/provider-client/review.py --use-claude

# Analyze specific file
python tools/code-review/provider-client/review.py path/to/file.py --use-claude

# Analyze git diff
python tools/code-review/provider-client/review.py --diff master..HEAD --use-claude

# Review PR (read-only)
python tools/code-review/provider-client/review.py --pr NUMBER --use-claude

# Review and post to GitHub
python tools/code-review/provider-client/review.py --pr NUMBER --use-claude --post-to-github

# Review and approve if clean
python tools/code-review/provider-client/review.py --pr NUMBER --use-claude --post-to-github --approve
```

---

## 🎯 What Gets Checked

The analyzer detects:

❌ **Missing context managers:**
```python
# BAD
address = get_provider_address()

# GOOD
with config.RunWithProviderConfigContextIfAvailable():
    address = get_provider_address()
```

❌ **Missing test markers:**
```python
# BAD
def test_provider():
    get_provider_address()

# GOOD
@runs_on_provider
def test_provider():
    get_provider_address()
```

---

## 💡 Example Output

**Basic Analysis:**
```
Found 2 issue(s):

1. tests/my_test.py:42:18
   [ERROR] missing-provider-context
   Call to 'get_provider_address' requires context manager
```

**With Claude AI:**
```
🤖 Claude Review:

1. tests/my_test.py:42
   [ERROR]

   This call to `get_provider_address()` is missing the required
   context manager.

   **Why:** Provider functions must run in provider cluster context
   to avoid accessing the wrong cluster.

   **Fix:**
   ```python
   with config.RunWithProviderConfigContextIfAvailable():
       address = get_provider_address()
   ```

   **Alternative:** Add `@runs_on_provider` to the test function.
```

---

## 🔧 Customization

### Add Your Own Patterns

Edit `tools/code-review/provider-client/analyzers/patterns.yaml`:

```yaml
provider_functions:
  - get_provider_address    # Already configured
  - my_custom_function      # Add your function
```

### Change Claude Model

Edit `tools/code-review/provider-client/analyzers/claude_reviewer.py`:

```python
self.model = "claude-sonnet-4-5@20250929"  # Default (fast, good)
# self.model = "claude-opus-4@20250514"    # Most capable
# self.model = "claude-haiku-4@20250313"   # Fastest, cheapest
```

---

## 🐛 Troubleshooting

### Error: "anthropic not installed"

```bash
pip install anthropic
```

### Error: "ANTHROPIC_API_KEY not set"

```bash
export ANTHROPIC_API_KEY="sk-ant-..."
# Or add to tools/code-review/provider-client/.env file
```

### Error: "gh not found"

Install GitHub CLI: https://cli.github.com/

```bash
# Ubuntu/Debian
sudo apt install gh

# macOS
brew install gh

# Fedora
sudo dnf install gh
```

Then authenticate:
```bash
gh auth login
```

### Workflow not running

1. Check workflow file exists: `.github/workflows/ai-code-review.yml`
2. Add `ANTHROPIC_API_KEY` to repository secrets
3. PR must modify `.py` files

---

## 💰 Cost

**Claude API** (Sonnet 4.5):
- $3 per million tokens
- Average PR: ~5,000 tokens = **$0.015** per review
- 50 PRs/month: **~$0.75/month**

**Very affordable!** Less than a coffee per month.

---

## 📚 Next Steps

### For Developers

1. ✅ Set up API key (done above)
2. Run review on your current branch
3. Fix any issues found
4. Use `--use-claude` for better suggestions

### For Repository Maintainers

1. ✅ Add `ANTHROPIC_API_KEY` to repo secrets
2. Workflow runs automatically
3. Customize patterns in `patterns.yaml`
4. Monitor costs in Anthropic console

### Learn More

- **[README.md](README.md)** - Full documentation
- **[SETUP.md](SETUP.md)** - Detailed setup guide
- **[DEMO.md](DEMO.md)** - Interactive demo
- **[EXAMPLE.md](EXAMPLE.md)** - Usage examples

---

## 🎉 You're Ready!

Start using AI-powered code review:

```bash
# Review your current work
python tools/code-review/provider-client/review.py --use-claude

# Or create a PR and get automatic review!
```

**Questions?** Check the docs or open an issue.

---

**🤖 Built with [Claude Code](https://claude.ai/code)**
