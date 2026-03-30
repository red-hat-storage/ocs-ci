# Architecture - AI-Powered Code Review System

Complete architecture overview of the Claude-powered Provider/Client pattern review system.

---

## 🏗️ System Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         GitHub PR Event                          │
│                    (opened, synchronized)                        │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│               GitHub Actions Workflow Runner                     │
│                (.github/workflows/ai-code-review.yml)            │
├─────────────────────────────────────────────────────────────────┤
│  1. Checkout code (fetch-depth: 0)                              │
│  2. Setup Python 3.11                                            │
│  3. Install dependencies (anthropic, pyyaml)                     │
│  4. Install GitHub CLI (gh)                                      │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                    ai_review_poc.py (Main CLI)                   │
│                                                                   │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ 1. Parse Arguments                                        │  │
│  │    --pr NUMBER --use-claude --post-to-github             │  │
│  └──────────────────────────────────────────────────────────┘  │
│                             │                                    │
│                             ▼                                    │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ 2. Initialize Components                                  │  │
│  │    • ProviderClientAnalyzer                              │  │
│  │    • ClaudeReviewer (if --use-claude)                   │  │
│  │    • GitHubReviewer (if --post-to-github)               │  │
│  └──────────────────────────────────────────────────────────┘  │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
         ┌───────────────────┴───────────────────┐
         │                                        │
         ▼                                        ▼
┌──────────────────────┐              ┌──────────────────────┐
│  Get PR Diff         │              │  Get File Content    │
│  (gh pr diff)        │              │  (for single file)   │
└──────┬───────────────┘              └──────┬───────────────┘
       │                                      │
       └─────────────┬────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────────┐
│          provider_client_analyzer.py (Pattern Detection)         │
│                                                                   │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ ProviderClientAnalyzer                                    │  │
│  │  • Load patterns.yaml                                     │  │
│  │  • Parse diff to get changed files/lines                 │  │
│  │  • For each changed Python file:                         │  │
│  └──────────────────────────────────────────────────────────┘  │
│                             │                                    │
│                             ▼                                    │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ ProviderPatternVisitor (AST Walker)                       │  │
│  │  • Parse file to AST                                      │  │
│  │  • Visit function definitions                             │  │
│  │  • Visit context managers (with statements)              │  │
│  │  • Visit function calls                                   │  │
│  │  • Track scope (in_provider_context)                     │  │
│  │  • Check decorators (@runs_on_provider)                  │  │
│  └──────────────────────────────────────────────────────────┘  │
│                             │                                    │
│                             ▼                                    │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ Pattern Matching                                          │  │
│  │  • Match function calls against provider_functions        │  │
│  │  • Check if in valid context manager                      │  │
│  │  • Check if in provider-marked test                       │  │
│  │  • Generate Finding objects                               │  │
│  └──────────────────────────────────────────────────────────┘  │
│                             │                                    │
│                             ▼                                    │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ Output: List[Finding]                                     │  │
│  │  • file_path, line_number, column                         │  │
│  │  • severity, rule, message                                │  │
│  │  • code_snippet, suggestion                               │  │
│  └──────────────────────────────────────────────────────────┘  │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
             ┌───────────────┴────────────────┐
             │  Are there findings?            │
             └───────────────┬────────────────┘
                             │
                    ┌────────┴────────┐
                    │ YES             │ NO
                    ▼                 ▼
    ┌───────────────────────┐   ┌──────────────────┐
    │ Format and Display    │   │ Show "All Clean" │
    │ Basic Findings        │   │ Message          │
    └───────┬───────────────┘   └─────────┬────────┘
            │                              │
            ▼                              │
  ┌─────────────────────┐                 │
  │  --use-claude?      │                 │
  └─────────┬───────────┘                 │
            │                              │
       ┌────┴────┐                        │
       │ YES     │ NO                     │
       ▼         └────────────────┐       │
┌─────────────────────────────────▼───────▼───────────────────────┐
│            claude_reviewer.py (AI Enhancement)                   │
│                                                                   │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ ClaudeReviewer                                            │  │
│  │  • Group findings by file                                 │  │
│  │  • For each file with findings:                           │  │
│  └──────────────────────────────────────────────────────────┘  │
│                             │                                    │
│                             ▼                                    │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ Build Prompt                                              │  │
│  │  • Add ocs-ci context (Provider/Client explanation)       │  │
│  │  • Include file path and findings                         │  │
│  │  • Add code snippets                                      │  │
│  │  • Request JSON-formatted output                          │  │
│  └──────────────────────────────────────────────────────────┘  │
│                             │                                    │
│                             ▼                                    │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ Call Claude API                                           │  │
│  │  • POST to Anthropic Messages API                         │  │
│  │  • Model: claude-sonnet-4-5@20250929                      │  │
│  │  • Max tokens: 4096                                       │  │
│  └──────────────────────────────────────────────────────────┘  │
│                             │                                    │
│                             ▼                                    │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ Parse Response                                            │  │
│  │  • Extract JSON array from markdown                       │  │
│  │  • Parse into ReviewComment objects                       │  │
│  │  • Fallback to basic comments on error                    │  │
│  └──────────────────────────────────────────────────────────┘  │
│                             │                                    │
│                             ▼                                    │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ Output: List[ReviewComment]                               │  │
│  │  • file_path, line_number                                 │  │
│  │  • body (markdown), severity                              │  │
│  │  • Includes: explanation, code example, why it matters    │  │
│  └──────────────────────────────────────────────────────────┘  │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
                  ┌──────────────────────┐
                  │ Display Claude Review │
                  │ (formatted markdown)  │
                  └──────────┬────────────┘
                             │
                             ▼
                  ┌──────────────────────┐
                  │ --post-to-github?    │
                  └──────────┬───────────┘
                             │
                    ┌────────┴────────┐
                    │ YES             │ NO
                    ▼                 ▼
┌─────────────────────────────────┐  │
│  github_integration.py          │  │
│  (Post to GitHub)               │  │
│                                  │  │
│  ┌────────────────────────────┐ │  │
│  │ GitHubReviewer             │ │  │
│  │ • Get PR info (gh pr view) │ │  │
│  │ • Determine review event   │ │  │
│  │   (REQUEST_CHANGES/APPROVE)│ │  │
│  └────────────────────────────┘ │  │
│              │                   │  │
│              ▼                   │  │
│  ┌────────────────────────────┐ │  │
│  │ Post Main Review           │ │  │
│  │ • gh pr review NUMBER      │ │  │
│  │ • Include summary comment  │ │  │
│  │ • Set status               │ │  │
│  └────────────────────────────┘ │  │
│              │                   │  │
│              ▼                   │  │
│  ┌────────────────────────────┐ │  │
│  │ Post Inline Comments       │ │  │
│  │ • gh api PR/comments       │ │  │
│  │ • One per ReviewComment    │ │  │
│  │ • At specific line numbers │ │  │
│  └────────────────────────────┘ │  │
│              │                   │  │
│              ▼                   │  │
│  ┌────────────────────────────┐ │  │
│  │ ✅ Posted to GitHub        │ │  │
│  └────────────────────────────┘ │  │
└─────────────┬───────────────────┘  │
              │                      │
              └──────────┬───────────┘
                         │
                         ▼
              ┌──────────────────────┐
              │ Print Summary        │
              │ • Errors count       │
              │ • Warnings count     │
              │ • PR link (if posted)│
              └──────────┬───────────┘
                         │
                         ▼
              ┌──────────────────────┐
              │ Exit Code            │
              │ • 0 if no errors     │
              │ • 1 if errors found  │
              └──────────────────────┘
```

---

## 🔄 Data Flow

### 1. Input Sources

```
┌─────────────┐  ┌─────────────┐  ┌─────────────┐
│ Git Diff    │  │ Single File │  │ PR Number   │
│ (git diff)  │  │ (Read file) │  │ (gh pr diff)│
└──────┬──────┘  └──────┬──────┘  └──────┬──────┘
       │                │                │
       └────────────────┼────────────────┘
                        │
                        ▼
                   ┌─────────┐
                   │ Analyzer│
                   └─────────┘
```

### 2. Pattern Detection Flow

```
Python Source Code
        │
        ▼
   Parse to AST
        │
        ▼
   Walk AST Tree
        │
    ┌───┴───┐
    │       │
    ▼       ▼
Function  Context
 Calls    Managers
    │       │
    ▼       ▼
  Check   Track
Patterns  Scope
    │       │
    └───┬───┘
        ▼
    Findings
```

### 3. Claude Review Flow

```
Findings (List)
        │
        ▼
Build Prompt
  • Add context
  • Format findings
  • Request structure
        │
        ▼
Send to Claude API
  • Model: sonnet-4-5
  • Max tokens: 4096
        │
        ▼
Receive Response
  • JSON array
  • Code examples
  • Explanations
        │
        ▼
Parse & Format
        │
        ▼
ReviewComments (List)
```

### 4. GitHub Integration Flow

```
ReviewComments
        │
        ▼
Get PR Info
  (gh pr view)
        │
        ▼
Determine Status
  • Errors? → REQUEST_CHANGES
  • Clean? → APPROVE (if --approve)
  • Otherwise → COMMENT
        │
        ▼
Post Main Review
  (gh pr review)
        │
        ▼
Post Inline Comments
  (gh api /comments)
  • One per ReviewComment
  • At specific lines
        │
        ▼
    ✅ Done
```

---

## 🧩 Component Interactions

```
┌──────────────────────────────────────────────────────────────┐
│                        ai_review_poc.py                       │
│                       (Orchestrator)                          │
└───┬──────────────────┬──────────────────┬──────────────────┬─┘
    │                  │                  │                  │
    │ uses             │ uses             │ uses             │ reads
    │                  │                  │                  │
    ▼                  ▼                  ▼                  ▼
┌─────────┐    ┌──────────────┐  ┌────────────┐   ┌──────────────┐
│Provider │    │   Claude     │  │  GitHub    │   │  patterns    │
│Client   │    │   Reviewer   │  │ Integration│   │  .yaml       │
│Analyzer │    │              │  │            │   │              │
└────┬────┘    └──────┬───────┘  └─────┬──────┘   └──────────────┘
     │                │                 │
     │ produces       │ produces        │ posts
     │                │                 │
     ▼                ▼                 ▼
┌─────────┐    ┌──────────────┐  ┌────────────┐
│Findings │───>│Review        │─>│ GitHub PR  │
│         │    │Comments      │  │ Comments   │
└─────────┘    └──────────────┘  └────────────┘
```

---

## 📦 Module Dependencies

```
ai_review_poc.py
    ├── provider_client_analyzer.py
    │   └── patterns.yaml (loaded via pyyaml)
    │
    ├── claude_reviewer.py
    │   ├── provider_client_analyzer.py (Finding class)
    │   └── anthropic (SDK)
    │
    └── github_integration.py
        ├── claude_reviewer.py (ReviewComment class)
        └── subprocess (gh CLI)

External Dependencies:
    • anthropic >= 0.39.0
    • pyyaml >= 6.0
    • subprocess (stdlib)
    • argparse (stdlib)
    • ast (stdlib)
```

---

## 🔐 Security Architecture

```
┌─────────────────────────────────────────────────────────┐
│                   Security Layers                        │
├─────────────────────────────────────────────────────────┤
│                                                          │
│  1. Environment Variables                               │
│     • ANTHROPIC_API_KEY (never in code)                │
│     • GITHUB_TOKEN (from GH secrets)                   │
│     • .env files in .gitignore                         │
│                                                          │
│  2. API Authentication                                  │
│     • Claude API: API key header                       │
│     • GitHub API: gh CLI authenticated                 │
│                                                          │
│  3. Permissions                                         │
│     • GitHub: read repo, write comments only           │
│     • Workflow: minimal required permissions           │
│                                                          │
│  4. Input Validation                                    │
│     • PR numbers validated                             │
│     • File paths sanitized                             │
│     • AST parsing (no code execution)                  │
│                                                          │
└─────────────────────────────────────────────────────────┘
```

---

## ⚡ Performance Characteristics

### Pattern Analyzer
- **Speed:** < 1 second for typical file
- **Scalability:** O(n) where n = lines of code
- **Memory:** Minimal (AST only)

### Claude API
- **Latency:** 2-5 seconds per request
- **Throughput:** Batches files efficiently
- **Cost:** ~$0.015 per review

### GitHub Integration
- **Latency:** 1-2 seconds for posting
- **Rate limits:** Respects GitHub API limits
- **Retries:** Graceful failure handling

### Overall
- **Total PR review time:** 30-60 seconds
- **Scales to:** 1000s of PRs/day
- **Bottleneck:** Claude API calls

---

## 🎯 Extension Points

### 1. Add New Patterns

```yaml
# patterns.yaml
provider_functions:
  - your_new_function  # Add here
```

### 2. Customize Claude Prompts

```python
# claude_reviewer.py
def _build_review_prompt(self, ...):
    # Modify prompt structure here
```

### 3. Add Analysis Rules

```python
# provider_client_analyzer.py
class ProviderPatternVisitor:
    def visit_NewNodeType(self, node):
        # Add new AST visitor
```

### 4. Custom GitHub Actions

```yaml
# .github/workflows/ai-code-review.yml
# Add steps or modify behavior
```

---

## 📊 Monitoring & Observability

### Logs

```
Console Output:
  • Analysis progress
  • Finding counts
  • Claude status
  • GitHub posting status
  • Error messages

GitHub Actions:
  • Workflow run logs
  • Step-by-step output
  • Error traces
```

### Metrics

```
Track:
  • Reviews per day
  • Findings per review
  • Claude API usage
  • GitHub API calls
  • Error rates

Monitor in:
  • Anthropic Console (API usage)
  • GitHub Actions (workflow runs)
  • Application logs
```

---

## 🔮 Future Architecture Improvements

### Phase 2: Enhanced Features

```
┌─────────────────────────────────────────────┐
│  Auto-fix Mode                              │
│  • Generate fix commits                     │
│  • Push to PR branch                        │
│  • Request review                           │
└─────────────────────────────────────────────┘

┌─────────────────────────────────────────────┐
│  Learning System                            │
│  • Track false positives                    │
│  • Refine patterns                          │
│  • Custom model fine-tuning                 │
└─────────────────────────────────────────────┘

┌─────────────────────────────────────────────┐
│  Analytics Dashboard                        │
│  • Review statistics                        │
│  • Cost tracking                            │
│  • Pattern trends                           │
└─────────────────────────────────────────────┘
```

### Phase 3: Platform Integration

```
┌──────────────┐    ┌──────────────┐    ┌──────────────┐
│ IDE Extension│    │ Slack Bot    │    │ Web UI       │
│ • VS Code    │    │ • Summaries  │    │ • Manual     │
│ • JetBrains  │    │ • Alerts     │    │   reviews    │
└──────────────┘    └──────────────┘    └──────────────┘
        │                   │                    │
        └───────────────────┴────────────────────┘
                            │
                            ▼
                   ┌────────────────┐
                   │  Core Review   │
                   │  Service (API) │
                   └────────────────┘
```

---

## 📝 Architecture Decisions

### Why AST Parsing?
- ✅ Accurate (no regex)
- ✅ Handles complex Python
- ✅ Low false positives
- ✅ Standard library (ast module)

### Why Claude API?
- ✅ Best-in-class code understanding
- ✅ Context-aware suggestions
- ✅ Structured output
- ✅ Cost-effective

### Why GitHub CLI?
- ✅ Simpler than REST API
- ✅ Handles auth automatically
- ✅ Well-documented
- ✅ Works in Actions

### Why YAML for Patterns?
- ✅ Human-readable
- ✅ Easy to edit
- ✅ Version controlled
- ✅ No code changes needed

---

**Architecture designed for simplicity, maintainability, and extensibility.**
