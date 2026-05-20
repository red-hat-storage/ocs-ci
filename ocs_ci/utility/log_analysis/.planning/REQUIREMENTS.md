# Requirements: OCS-CI Log Analysis — NVIDIA NIM Backend

**Defined:** 2026-04-23
**Core Value:** Run the full log analysis pipeline through NVIDIA NIM models via `--ai-backend nim`

## v1 Requirements

### Backend

- [ ] **BACK-01**: `NimBackend` class subclasses `ClaudeCodeBackend` and injects LiteLLM proxy env vars into subprocess environment
- [ ] **BACK-02**: Backend registered in `get_backend()` factory as `"nim"`
- [ ] **BACK-03**: `--ai-backend nim` added to CLI choices in `cli.py`
- [ ] **BACK-04**: Framework config supports `ai_backend: "nim"` in `LOG_ANALYSIS` section
- [ ] **BACK-05**: Non-agentic classification produces valid structured JSON through NIM
- [ ] **BACK-06**: Agentic investigation with Bash/Read tool use works through NIM
- [ ] **BACK-07**: Run summary generation works through NIM

## v2 Requirements

None — single-phase project.

## Out of Scope

| Feature | Reason |
|---------|--------|
| LiteLLM proxy setup/deployment | User manages separately via Docker |
| Prompt modifications for NIM models | `drop_params: true` handles API differences |
| Anthropic SDK backend migration | Only Claude Code CLI path is being migrated |
| New agentic loop implementation | Reusing existing Claude Code CLI approach |
| Configurable proxy URL | Always localhost:4000, can add later if needed |

## Traceability

| Requirement | Phase | Status |
|-------------|-------|--------|
| BACK-01 | Phase 1 | Pending |
| BACK-02 | Phase 1 | Pending |
| BACK-03 | Phase 1 | Pending |
| BACK-04 | Phase 1 | Pending |
| BACK-05 | Phase 1 | Pending |
| BACK-06 | Phase 1 | Pending |
| BACK-07 | Phase 1 | Pending |

**Coverage:**
- v1 requirements: 7 total
- Mapped to phases: 7
- Unmapped: 0 ✓

---
*Requirements defined: 2026-04-23*
*Last updated: 2026-04-23 after initial definition*
