
---

## `workflow.md`

```md
# Workflow Rules

This document defines how development should be performed inside this repository.

The goal is to maintain architectural consistency, reduce unnecessary complexity, and prevent context drift during AI-assisted development.

---

# Development Philosophy

- Build incrementally
- Prefer simple systems over clever systems
- Keep architecture understandable
- Avoid premature abstraction
- Prioritize operational reliability
- Prefer correctness before optimization
- Avoid speculative engineering

---

# AI Development Constraints

The AI assistant must:

- Read relevant files completely before modifying code
- Understand existing architecture before implementation
- Avoid introducing unrelated changes
- Avoid unnecessary rewrites
- Avoid changing working systems without justification
- Modify only necessary components
- Keep changes localized and incremental

---

# Before Writing Code

Before implementing any feature:

1. Read all relevant files
2. Understand current architecture and data flow
3. Explain planned changes briefly
4. Identify affected services/modules
5. Avoid speculative future abstractions
6. Prefer extending existing systems over replacing them

---

# Implementation Rules

## General Rules

- Keep functions focused and readable
- Prefer explicit code over overly abstract code
- Avoid unnecessary interfaces or patterns
- Avoid deep inheritance structures
- Avoid premature microservices
- Favor operational simplicity

---

# Distributed Systems Rules

The system should prioritize:

- asynchronous event processing
- idempotent consumers
- retry-safe operations
- observable failures
- scalable workers
- low coupling between components

Avoid:
- tightly coupled service orchestration
- synchronous chains across services
- hidden background behavior
- silent failure handling

---

# Kafka Rules

- Topics should have clear ownership
- Event schemas should remain consistent
- Consumers must tolerate duplicate delivery
- Avoid embedding business logic inside Kafka adapters
- Prefer explicit event payloads

---

# Redis Rules

Redis should primarily be used for:

- rankings
- counters
- hot aggregations
- caching
- low-latency reads

Avoid:
- storing critical long-term data only in Redis
- overly complex Redis data models

---

# PostgreSQL Rules

Postgres should store:

- persistent analytics
- historical records
- durable aggregated data
- metadata

Avoid:
- unnecessary normalization early
- premature optimization of schemas

---

# Frontend Rules

The frontend is an operational dashboard.

Prioritize:
- data visibility
- real-time updates
- responsiveness
- clear metrics display

Avoid:
- excessive animations
- unnecessary state complexity
- UI-heavy architecture early

---

# File Modification Rules

- Do not rewrite entire files unnecessarily
- Preserve existing architecture
- Avoid unrelated formatting changes
- Keep diffs focused and readable
- Do not rename files without justification
- Avoid introducing large dependencies casually

---

# Refactoring Rules

Refactoring is allowed ONLY if:

- it improves clarity significantly
- it reduces operational complexity
- it removes duplicated logic
- it improves reliability

Do NOT refactor purely for stylistic reasons.

---

# Error Handling Rules

- Never silently swallow errors
- Errors should be observable
- Failures should include actionable logs
- Prefer explicit failure handling
- Retry logic should be bounded and visible

---

# Logging Rules

Logs should:

- provide operational value
- include contextual identifiers
- avoid unnecessary verbosity
- avoid leaking sensitive information

Prefer structured logging when possible.

---

# Verification Process

After implementation:

1. Verify project compiles
2. Verify imports are valid
3. Verify lint passes
4. Verify environment variables are documented
5. Verify Docker compatibility
6. Verify no unrelated systems broke
7. Verify APIs remain consistent
8. Verify event flow still functions

---

# Pull Request Style Changes

Each implementation should aim for:

- small focused changes
- readable diffs
- isolated features
- minimal architectural disruption

Avoid:
- giant rewrites
- unrelated cleanup commits
- speculative features

---

# Completion Report Format

After completing work, provide:

## Files Modified

## What Changed

## Why The Change Was Needed

## Architectural Impact

## Risks / Concerns

## Remaining Work

---

# Preferred Development Order

When adding new systems:

1. Event schema
2. Kafka producer
3. Kafka consumer
4. Aggregation/storage
5. API layer
6. Frontend visualization
7. Observability
8. Reliability improvements

---

# Long-Term Engineering Direction

The long-term goal is to demonstrate:

- distributed systems engineering
- event-driven architecture
- scalable backend infrastructure
- observability
- reliability engineering
- operational thinking

All development decisions should align with these priorities.