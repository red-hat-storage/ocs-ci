# OCS-CI Developer Tools

Collection of development tools for the ocs-ci project.

## 📂 Directory Structure

```
tools/
├── code-review/          # Code review and analysis tools
│   ├── provider-client/  # Provider/Client pattern review (AI-powered)
│   ├── rgw/             # Future: RGW pattern review
│   ├── mcg/             # Future: MCG pattern review
│   └── disaster-recovery/ # Future: DR pattern review
│
├── deployment/          # Future: Deployment automation tools
├── testing/             # Future: Test utilities
└── README.md            # This file
```

## 🛠️ Available Tools

### Code Review Tools

| Tool | Purpose | Status | Docs |
|------|---------|--------|------|
| [provider-client](code-review/provider-client/) | AI-powered Provider/Client pattern review | ✅ Ready | [📖 Docs](code-review/provider-client/docs/) |
| rgw | RGW/Object storage pattern review | 📋 Planned | - |
| mcg | Multi-Cloud Gateway pattern review | 📋 Planned | - |
| disaster-recovery | DR workflow pattern review | 📋 Planned | - |

## 🚀 Quick Start

### Provider/Client Pattern Review

```bash
# Install dependencies
pip install -r tools/code-review/provider-client/requirements.txt

# Set API key
export ANTHROPIC_API_KEY="your-key"

# Run review
python tools/code-review/provider-client/review.py --use-claude
```

See [code-review/provider-client/docs/](code-review/provider-client/docs/QUICKSTART.md) for details.

## 🤝 Contributing New Tools

Want to add a new developer tool? Follow this guide:

### 1. Choose the Right Category

- **code-review/** - Pattern analysis, linting, review automation
- **deployment/** - Cluster deployment, configuration tools
- **testing/** - Test utilities, data generators, helpers
- **Other** - Create new category if needed

### 2. Create Your Tool Directory

```bash
# Example: Adding RGW pattern review tool
mkdir -p tools/code-review/rgw
cd tools/code-review/rgw

# Create basic structure
touch review.py
touch requirements.txt
mkdir docs
```

### 3. Follow the Standard Structure

```
your-tool/
├── review.py (or main script)
├── requirements.txt
├── .env.example (if needed)
├── analyzers/ (if complex)
├── docs/
│   ├── README.md
│   └── QUICKSTART.md
└── tests/ (optional)
```

### 4. Document Your Tool

Create `docs/README.md` with:
- What the tool does
- Quick start guide
- Usage examples
- Configuration options

### 5. Update This Index

Add your tool to the table above with:
- Tool name (linked to directory)
- Purpose (1-line description)
- Status (✅ Ready, 🚧 In Progress, 📋 Planned)
- Link to docs

### 6. Add GitHub Workflow (Optional)

If your tool should run in CI:

```yaml
# .github/workflows/your-tool.yml
name: Your Tool Name
on: [pull_request]
jobs:
  run:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Run your tool
        run: python tools/your-category/your-tool/review.py
```

## 📋 Tool Guidelines

### Naming Conventions

- **Directories:** Use kebab-case (e.g., `provider-client`, `disaster-recovery`)
- **Scripts:** Short and descriptive (e.g., `review.py`, `check.py`, `generate.py`)
- **Docs:** Use standard names (README.md, QUICKSTART.md, SETUP.md)

### Code Standards

- Follow ocs-ci coding guidelines
- Include type hints where possible
- Add comprehensive docstrings
- Handle errors gracefully
- Provide helpful error messages

### Documentation

Each tool must include:
- **README.md** - Full documentation
- **QUICKSTART.md** - 5-minute getting started
- **Usage examples** - Common scenarios
- **Requirements** - Dependencies and prerequisites

### Testing

- Add unit tests where applicable
- Test on real ocs-ci code
- Document test usage
- Include test data/fixtures if needed

## 🎯 Tool Ideas

Looking for ideas? Consider building:

### Code Review Tools
- **RGW Pattern Reviewer** - Check RGW/object storage patterns
- **MCG Pattern Reviewer** - Verify Multi-Cloud Gateway usage
- **DR Workflow Checker** - Validate disaster recovery workflows
- **Storage Class Validator** - Check storage class configurations

### Deployment Tools
- **Config Generator** - Generate cluster configs from templates
- **Deployment Validator** - Pre-flight checks before deployment
- **Resource Calculator** - Estimate resource requirements

### Testing Tools
- **Test Data Generator** - Create sample PVs, PVCs, pods
- **Log Analyzer** - Parse and analyze test logs
- **Performance Reporter** - Generate performance reports
- **Flaky Test Detector** - Identify unstable tests

## 🔗 Related Resources

- [OCS-CI Documentation](../docs/)
- [Contributing Guidelines](../CONTRIBUTING.md)
- [Coding Standards](../docs/coding_guidelines.md)

---

**Questions?** Open an issue or check existing tool documentation.

**Built with ❤️ by the ocs-ci community**
