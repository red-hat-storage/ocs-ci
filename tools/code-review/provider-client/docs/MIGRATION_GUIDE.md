# Migration Guide - New Directory Structure

## 📁 What Changed

The Provider/Client review tool has been reorganized to support multiple code review tools.

### Old Structure
```
ci/
├── ai_review_poc.py
├── analyzers/
├── github_integration.py
├── requirements-ai-review.txt
└── *.md (docs)
```

### New Structure
```
tools/
├── README.md (index of all tools)
└── code-review/
    ├── README.md (index of review tools)
    └── provider-client/
        ├── review.py (renamed from ai_review_poc.py)
        ├── analyzers/
        ├── github_integration.py
        ├── requirements.txt
        └── docs/ (all documentation)
```

---

## 🔄 Path Updates

| Old Path | New Path |
|----------|----------|
| `ci/ai_review_poc.py` | `tools/code-review/provider-client/review.py` |
| `ci/requirements-ai-review.txt` | `tools/code-review/provider-client/requirements.txt` |
| `ci/analyzers/` | `tools/code-review/provider-client/analyzers/` |
| `ci/*.md` | `tools/code-review/provider-client/docs/*.md` |
| `.github/workflows/ai-code-review.yml` | `.github/workflows/provider-client-review.yml` |

---

## 🚀 How to Update Your Workflow

### If You Run Locally

**Old command:**
```bash
python ci/ai_review_poc.py --use-claude
```

**New command:**
```bash
python tools/code-review/provider-client/review.py --use-claude
```

### If You Have Local Scripts

Update any scripts or aliases that reference the old path:

```bash
# Old
alias review="python ci/ai_review_poc.py --use-claude"

# New
alias review="python tools/code-review/provider-client/review.py --use-claude"
```

### If You Have Custom Workflows

Update your `.github/workflows/*.yml` files:

```yaml
# Old
- run: python ci/ai_review_poc.py --pr ${{ github.event.pull_request.number }}

# New
- run: python tools/code-review/provider-client/review.py --pr ${{ github.event.pull_request.number }}
```

Also update requirements path:

```yaml
# Old
- run: pip install -r ci/requirements-ai-review.txt

# New
- run: pip install -r tools/code-review/provider-client/requirements.txt
```

---

## 🔧 Import Updates

If you've extended the tool with custom code:

**Old imports:**
```python
from ci.analyzers.provider_client_analyzer import ProviderClientAnalyzer
from ci.analyzers.claude_reviewer import ClaudeReviewer
from ci.github_integration import GitHubReviewer
```

**New imports:**
```python
from tools.code_review.provider_client.analyzers.provider_client_analyzer import ProviderClientAnalyzer
from tools.code_review.provider_client.analyzers.claude_reviewer import ClaudeReviewer
from tools.code_review.provider_client.github_integration import GitHubReviewer
```

---

## ✅ What Stays the Same

- **Environment variables:** Still use `ANTHROPIC_API_KEY` and `GITHUB_TOKEN`
- **Configuration:** `.env` file location (now in tool directory)
- **Patterns:** `analyzers/patterns.yaml` structure unchanged
- **Functionality:** All features work exactly the same
- **Command-line options:** Same flags and arguments

---

## 🎯 Why This Change?

### Better Organization
- Clear separation: developer tools in `tools/`
- Scalable: easy to add more review tools
- Discoverable: obvious where to find code review tools

### Supports Multiple Tools

Now developers can add specialized review tools:

```
tools/code-review/
├── provider-client/  # This tool
├── rgw/              # Future: RGW patterns
├── mcg/              # Future: MCG patterns
└── disaster-recovery/ # Future: DR patterns
```

### Clearer Purpose

- `ci/` was ambiguous (CI config? CI scripts?)
- `tools/code-review/provider-client/` is self-documenting
- Easy for new contributors to understand

---

## 📚 Updated Documentation

All documentation has been updated with new paths:

- ✅ README.md - Main documentation
- ✅ QUICKSTART.md - 5-minute setup
- ✅ SETUP.md - Detailed guide
- ✅ EXAMPLE.md - Usage examples
- ✅ DEMO.md - Interactive demo
- ✅ ARCHITECTURE.md - System design
- ✅ GET_STARTED.txt - Quick reference

---

## 🆘 Troubleshooting

### "Module not found" Error

**Problem:**
```
ModuleNotFoundError: No module named 'ci.analyzers'
```

**Solution:**
You're using old imports. Update to new paths (see "Import Updates" above).

### Workflow Not Running

**Problem:**
GitHub Actions workflow doesn't trigger.

**Solution:**
1. Check workflow is named `.github/workflows/provider-client-review.yml`
2. Verify paths in workflow use `tools/code-review/provider-client/`
3. Make sure `ANTHROPIC_API_KEY` secret is still set

### Can't Find review.py

**Problem:**
```
python: can't open file 'ci/ai_review_poc.py'
```

**Solution:**
Use new path: `tools/code-review/provider-client/review.py`

---

## 🤝 Need Help?

- Check [README.md](README.md) for full documentation
- See [QUICKSTART.md](QUICKSTART.md) for setup
- Review [SETUP.md](SETUP.md) for troubleshooting

---

**Migration complete! The tool now lives at `tools/code-review/provider-client/` 🎉**
