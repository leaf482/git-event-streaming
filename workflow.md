# Git Workflow Rules

- Create small focused commits
- Commit after each logically complete unit of work
- Avoid large multi-purpose commits
- Write clear operational commit messages
- Never mix refactors with feature implementation unnecessarily
- Keep commits easy to rollback
- Prefer incremental infrastructure evolution

---

# Milestone Commit And Push Rules

When a development phase or clearly defined milestone is complete, the AI assistant should commit and push the completed work without waiting for a separate prompt.

Before committing, the assistant must:

- run `git status` to review all changed and untracked files
- run `git diff` to understand the exact changes being committed
- scan for secrets or sensitive values, including tokens, passwords, private keys, credentials, and real `.env` files
- confirm that only example placeholders are present in committed env templates
- avoid committing local secret files such as `.env`, `.env.production`, credentials files, private keys, or generated backups
- run the relevant verification commands for the completed milestone

After the safety check passes, the assistant should:

- create a focused commit with an operational commit message
- push the current branch to the configured remote
- report the commit hash, pushed branch, verification commands, and secret-scan result

If sensitive information is detected, the assistant must stop, do not commit, do not push, and report the exact file paths that need attention.

---

# Branching Strategy

- main should remain deployable
- Use short-lived feature branches
- Merge only after local verification
- Avoid long-running unstable branches

Suggested branch naming:

- feature/kafka-consumer
- feature/aws-deployment
- feature/redis-aggregation
- fix/healthcheck-timeout
- refactor/logging-cleanup

---

# Pull Request Rules

Each PR should:

- focus on one architectural concern
- explain operational impact
- explain deployment impact
- explain risks
- remain reasonably small and reviewable

PRs should include:

## Summary
## Files Changed
## Architectural Changes
## Deployment Impact
## Risks / Rollback Plan
## Verification Steps

---

# Commit Message Style

Prefer concise infrastructure-oriented commits.

Examples:

- add github event normalization pipeline
- add kafka producer with structured logging
- add redis aggregation worker
- optimize docker compose for ec2 deployment
- add container healthchecks and restart policies
- implement idempotent consumer handling

Avoid vague commits like:
- fixes
- update stuff
- changes