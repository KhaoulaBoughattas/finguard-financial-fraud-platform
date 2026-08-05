# ADR 011: Security & Secrets Management

## Context
Phase 0 specifies: never hardcode credentials, non-root Docker containers,
dependency scanning, input validation, an API authentication
concept/documentation, and least-privilege principles. Phase 9 had to
decide what's genuinely auditable/executable in this sandbox (secrets grep,
dependency scan, API auth logic) versus what can only be documented as a
production target (real container builds, a real secrets manager, TLS).

## Decision: run real audits, not just write policy documentation
Rather than only documenting what security practices *should* look like,
this phase actually executed a secrets grep across the whole repository
and a real `pip-audit` dependency scan — both produce falsifiable
results, not descriptions of intent. This is consistent with this
project's standing principle (established since Phase 2's "NOT RUN" vs.
"PASS" distinction) that a claim is only as good as what was actually
executed. The secrets grep found zero hardcoded credentials (a real,
verified negative result, not an assumption); `pip-audit` found two real
CVEs, both fixed and re-verified against the full test suite.

## Decision: upgrade vulnerable dependencies rather than accept risk,
verified against the full test suite
Both `pyarrow` and `pytest` had low-actual-severity findings (the pyarrow
advisory explicitly states the vulnerable path isn't reachable from
Python bindings; pytest's issue is a dev-only local-user vector). Chose
to upgrade anyway, since safe fix versions existed — "low severity" is not
the same as "no reason to fix," and leaving a known CVE unpatched when a
compatible fix exists sets a bad precedent regardless of this specific
instance's real-world exploitability. Critically: **the upgrade was only
accepted after rerunning the full 144-test suite and confirming zero
regressions** — an untested dependency bump is itself a risk, just a
different kind.

## Decision: single shared API key, not OAuth2/JWT, for this phase
Phase 0 asks for an "API authentication concept/documentation." A single
shared `X-API-Key` header, checked with a constant-time comparison, is
honestly scoped to what a portfolio/demo project needs to *demonstrate*
the concept correctly (auth can be required, missing/wrong keys are
rejected, public endpoints stay public) without building the additional
infrastructure (a user/client identity store, token issuance, refresh
flows) that a real multi-tenant production API would need and that this
project has no consumer for yet. Documented explicitly as NOT the
production target (`docs/security.md §5`) — OAuth2/JWT or Azure API
Management, matching this project's established Azure architecture.

## Decision: `/health` and `/metrics` stay unauthenticated
`/health` must remain reachable without a secret so orchestration systems
(Docker healthchecks, a future Kubernetes liveness probe) can check it
without provisioning credentials into infrastructure tooling. `/metrics`
follows the common Prometheus operational pattern of network-level
isolation (firewall the port) rather than per-scrape authentication,
since Prometheus's default scrape configuration doesn't send custom
headers. Both are real, considered decisions, not omissions — verified
directly: a test confirms `/health` returns 200 with zero API key even
when one IS configured for the other endpoints.

## Decision: non-root Docker container fix, unbuilt
`infra/docker/Dockerfile.api` ran as root with no functional need to
(no privileged port, no host-filesystem writes) — fixed with a dedicated
`useradd`/`USER` step. Could not be build-tested (no Docker daemon, same
limitation as every container-related decision since Phase 2) — the fix
is syntactically standard and low-risk, but explicitly flagged as
unverified rather than claimed working.

## Bug found and accepted, not silently ignored: Postgres role has
CREATEDB
Reviewing the `finguard` role's actual privileges (`\du finguard`) surfaced
that it holds `CREATEDB` — granted in Phase 6 purely for local convenience
(creating the separate `airflow` metadata database without provisioning a
second Postgres user). This is broader than the running application
needs and would be a real least-privilege violation in production. **Not
fixed in this phase** — doing so would require re-provisioning the shared
local Postgres setup multiple earlier phases already depend on and have
verified against, risking breaking working state for a fix whose real
audience is the production target, not this sandbox. Documented
explicitly as a known gap with the correct production fix
(`docs/security.md §6`: separate app role vs. migration/admin role)
rather than either silently leaving it unexamined or claiming a fix that
wasn't actually applied and verified.

## Local vs. Production
| | Local (this repo) | Production target |
|---|---|---|
| Secrets | `.env` + `pydantic-settings` | Azure Key Vault |
| API auth | Single shared API key, constant-time comparison | OAuth2/JWT or Azure API Management |
| Postgres roles | One role, `CREATEDB` granted (documented gap) | Separate least-privilege app role + migration/admin role |
| Container user | Non-root (fixed this phase, unbuilt) | Same, verified in a real CI/build pipeline |
| Transport | Plain HTTP locally | TLS everywhere (matches Kafka's documented TLS+SASL target) |

## Known Limitations
- API auth is a single shared secret, not per-client identity.
- Non-root container fix unbuilt (no Docker).
- Postgres least-privilege gap documented, not fixed, to avoid
  destabilizing already-verified earlier-phase state.
- No TLS locally; no rate limiting (unchanged gap since Phase 7).

## Consequences
- Any future phase adding a new sensitive endpoint should add
  `dependencies=[require_auth]` to its route decorator, following the
  pattern established here — not invent a second auth mechanism.
- Any future phase provisioning Postgres roles for real deployment should
  implement the separate app/migration role split documented in
  `docs/security.md §6`, since this phase found but deliberately deferred
  that fix.
- `requirements.txt`'s `pyarrow`/`pytest` version comments
  (`# CVE ... fixed, see docs/security.md §3`) are the pattern any future
  security-driven dependency bump should follow — a version pin with an
  inline reason, not a silent number change.
