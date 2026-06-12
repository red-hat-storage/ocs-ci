# Code Review Tools

AI-powered and automated code review tools for ocs-ci pattern validation.

## 🎯 Overview

This directory contains tools that automatically review code for pattern violations, best practices, and common mistakes specific to different areas of ocs-ci.

## 📦 Available Tools

### ✅ Provider/Client Pattern Review

**Location:** `provider-client/`
**Status:** Production Ready
**Technology:** Claude AI + Static Analysis

Reviews code for correct usage of Provider/Client mode patterns:
- Detects missing `RunWithProviderConfigContextIfAvailable()` context managers
- Identifies missing `@runs_on_provider` test markers
- Provides intelligent, context-aware suggestions via Claude AI
- Automatically posts review comments to GitHub PRs

**Quick Start:**
```bash
python tools/code-review/provider-client/review.py --use-claude
```

📚 **Documentation:** [provider-client/docs/](provider-client/docs/README.md)

---

### 📋 Planned Tools

#### RGW Pattern Review
**Purpose:** Validate RGW/object storage usage patterns
**Checks:**
- Correct bucket creation and deletion
- Proper S3 client configuration
- Resource cleanup patterns

#### MCG Pattern Review
**Purpose:** Check Multi-Cloud Gateway usage
**Checks:**
- NooBaa resource management
- Bucket class configurations
- OBC/OB creation patterns

#### Disaster Recovery Review
**Purpose:** Validate DR workflow implementations
**Checks:**
- Failover/relocate patterns
- DRPolicy configurations
- Multi-cluster context switching

---

## 🚀 Usage Patterns

### Running Locally

```bash
# Run specific review tool
python tools/code-review/<tool-name>/review.py [options]

# Example: Provider/Client review
python tools/code-review/provider-client/review.py --use-claude

# Review specific file
python tools/code-review/provider-client/review.py tests/my_test.py --use-claude

# Review PR
python tools/code-review/provider-client/review.py --pr 1234 --use-claude
```

### GitHub Actions Integration

Tools can run automatically in GitHub Actions:

```yaml
# .github/workflows/provider-client-review.yml
name: Provider/Client Pattern Review
on: [pull_request]
jobs:
  review:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Run review
        run: python tools/code-review/provider-client/review.py --pr ${{ github.event.pull_request.number }}
```

---

## 🛠️ Creating New Review Tools

### 1. Tool Structure

```
your-tool/
├── review.py              # Main script
├── analyzers/             # Pattern detection logic
│   ├── pattern_analyzer.py
│   ├── patterns.yaml      # Configuration
│   └── ai_reviewer.py     # Optional: AI integration
├── requirements.txt       # Dependencies
├── .env.example          # Environment template
├── docs/
│   ├── README.md
│   ├── QUICKSTART.md
│   ├── SETUP.md
│   └── EXAMPLES.md
└── tests/                # Unit tests
```

### 2. Implementation Checklist

- [ ] **Pattern Detection**
  - [ ] Define patterns to check (YAML config)
  - [ ] Implement AST-based analyzer
  - [ ] Add exemption rules
  - [ ] Test on real code

- [ ] **AI Integration** (Optional)
  - [ ] Claude API integration
  - [ ] Prompt engineering
  - [ ] Response parsing
  - [ ] Fallback handling

- [ ] **GitHub Integration** (Optional)
  - [ ] PR comment posting
  - [ ] Review status management
  - [ ] Summary generation

- [ ] **Documentation**
  - [ ] README with overview
  - [ ] Quick start guide
  - [ ] Setup instructions
  - [ ] Usage examples

- [ ] **Testing**
  - [ ] Unit tests for analyzer
  - [ ] Integration tests
  - [ ] Test on sample code

### 3. Best Practices

**Pattern Detection:**
- Use AST parsing over regex when possible
- Keep false positive rate low
- Provide clear, actionable error messages
- Include code examples in suggestions

**AI Integration:**
- Use structured prompts
- Request JSON-formatted responses
- Handle API failures gracefully
- Implement fallback to basic suggestions
- Keep prompts focused and specific

**Performance:**
- Analyze only changed lines in PRs
- Batch API requests when possible
- Cache results if appropriate
- Fail fast on errors

**User Experience:**
- Clear progress indicators
- Helpful error messages
- Examples in documentation
- Quick start under 5 minutes

### 4. Example: Minimal Review Tool

```python
#!/usr/bin/env python3
"""
Your Tool Name - Brief description

Usage:
    python tools/code-review/your-tool/review.py [file]
"""

import argparse
import sys

def analyze_file(file_path):
    """Analyze a single file for patterns."""
    findings = []

    # Your analysis logic here
    with open(file_path, 'r') as f:
        for line_num, line in enumerate(f, 1):
            if 'pattern_to_detect' in line:
                findings.append({
                    'line': line_num,
                    'message': 'Pattern violation detected',
                    'suggestion': 'Use correct_pattern instead'
                })

    return findings

def main():
    parser = argparse.ArgumentParser(description='Your tool description')
    parser.add_argument('file', help='File to analyze')
    args = parser.parse_args()

    findings = analyze_file(args.file)

    if findings:
        print(f"Found {len(findings)} issue(s):")
        for f in findings:
            print(f"  Line {f['line']}: {f['message']}")
            print(f"    Suggestion: {f['suggestion']}")
        return 1
    else:
        print("✅ No issues found!")
        return 0

if __name__ == '__main__':
    sys.exit(main())
```

---

## 📊 Tool Comparison

| Feature | Provider/Client | RGW (Planned) | MCG (Planned) | DR (Planned) |
|---------|----------------|---------------|---------------|--------------|
| **AI-Powered** | ✅ Claude | 📋 | 📋 | 📋 |
| **Static Analysis** | ✅ | 📋 | 📋 | 📋 |
| **GitHub Integration** | ✅ | 📋 | 📋 | 📋 |
| **PR Comments** | ✅ | 📋 | 📋 | 📋 |
| **Pattern Config** | ✅ YAML | 📋 | 📋 | 📋 |
| **Cost** | ~$1/month | Free | Free | Free |

---

## 🔗 Resources

- **Provider/Client Tool Docs:** [provider-client/docs/](provider-client/docs/)
- **Main Tools Directory:** [../](../)
- **OCS-CI Docs:** [../../docs/](../../docs/)
- **Contributing Guide:** [../../CONTRIBUTING.md](../../CONTRIBUTING.md)

---

## 🤝 Contributing

Want to add a new review tool? Check the guidelines in [../README.md](../README.md).

**Ideas for new tools:**
- Storage class validation
- Encryption pattern checks
- Performance test patterns
- UI test patterns
- Multi-cluster operation validation

---

**Questions?** Open an issue or check existing tool documentation.
