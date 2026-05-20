# Roadmap: NVIDIA NIM Backend

## Overview

Single-phase project: add a `nim` AI backend to the OCS-CI log analysis tool by subclassing `ClaudeCodeBackend` and injecting LiteLLM proxy environment variables. The existing Claude Code CLI subprocess handles all model interaction -- only the routing changes.

## Phases

- [ ] **Phase 1: Add NIM Backend** - Subclass ClaudeCodeBackend with LiteLLM proxy env vars, register in factory/CLI/config, verify all pipeline modes work

## Phase Details

### Phase 1: Add NIM Backend
**Goal**: Users can run the full log analysis pipeline through NVIDIA NIM models via `--ai-backend nim`
**Depends on**: Nothing (first phase)
**Requirements**: BACK-01, BACK-02, BACK-03, BACK-04, BACK-05, BACK-06, BACK-07
**Success Criteria** (what must be TRUE):
  1. Running `--ai-backend nim` selects the NIM backend and routes requests through the LiteLLM proxy at localhost:4000
  2. Non-agentic log classification produces valid structured JSON output through NIM (same schema as Claude)
  3. Agentic must-gather investigation with Bash/Read tool use works through NIM (same prompts, same tools)
  4. Run summary generation works through NIM
  5. Existing `claude-code` and `anthropic` backends continue to work unchanged
**Plans:** 1 plan

Plans:
- [x] 01-01-PLAN.md — Create NimBackend class, register in factory/CLI/config, verify all pipeline modes

## Progress

| Phase | Plans Complete | Status | Completed |
|-------|----------------|--------|-----------|
| 1. Add NIM Backend | 0/1 | Not started | - |
