# Security & Secrets Management (Phase 9)

## 1. What was actually done vs. documented target

```text
Secrets audit (grep across the whole repo):    RUN — zero hardcoded credentials found
Dependency vulnerability scan (pip-audit):      RUN — 2 real CVEs found and fixed
API key authentication:                          IMPLEMENTED and tested (14 tests, real requests)
Non-root Docker container (API image):           WRITTEN, not build-tested (no Docker)
Postgres least-privilege review:                 RUN — 1 real finding, documented below
TLS / SASL / secrets-manager (Kafka, Postgres):  DOCUMENTED as production target (unchanged from Phase 2)
```
Same posture as every prior phase: what could be run in this sandbox was
run for real; what needs infrastructure this sandbox doesn't have
(a live Docker daemon to build/run the non-root container, a real secrets
manager) is documented as the production target, not silently assumed
done.

## 2. Secrets Management

**Audit method**: `grep -rnE` across every `.py`/`.yml`/`.yaml`/`.sql`/`.sh`
file for password/secret/api-key patterns, filtered to exclude the
legitimate `settings.password`-style references and `.env.example`
placeholders. **Result: zero hardcoded credentials found** — every
credential in this codebase (Postgres password, Grafana admin password,
API key) flows through a `pydantic-settings` config class reading from
`.env`, consistent from Phase 1 onward. `.env` itself is gitignored and
confirmed not tracked.

**API key**: `SECURITY_API_KEY` (see `src/api/auth.py`). Defaults to
empty in `.env.example` — a deliberate local-dev-only posture, not a
production recommendation. When empty, every request is allowed through
AND a warning is logged once per process, so a developer who forgets to
set it notices rather than assuming protection that isn't there. When
set, `X-API-Key` is required and checked with `secrets.compare_digest`
(constant-time comparison — avoids a timing side-channel that could let
an attacker infer the key byte-by-byte from response latency).

**Production target**: Azure Key Vault, matching the target already
documented for Kafka (`docs/kafka.md §9`) and the rest of this project's
Azure architecture — application code would read secrets via the Key
Vault SDK or injected environment variables at deploy time, never from a
committed file. `.env`/`pydantic-settings` is the correct LOCAL
equivalent, not a placeholder that should look like Key Vault -- the
abstraction (a `Settings` class, not scattered `os.environ` calls) is
what makes swapping the actual source trivial later.

## 3. Dependency Vulnerability Scanning

Ran `pip-audit` against `requirements.txt` for real. **Found 2 genuine
CVEs**:

| Package | Version | CVE | Fixed in | Action taken |
|---|---|---|---|---|
| pyarrow | 18.0.0 | PYSEC-2026-113 (Use-After-Free in Arrow C++ IPC reader with pre-buffering) | 23.0.1 | Upgraded to 23.0.1 |
| pytest | 8.3.3 | PYSEC-2026-1845 (predictable `/tmp/pytest-of-{user}` directory, local privilege issue) | 9.0.3 | Upgraded to 9.0.3 |

Both were low actual risk in this codebase specifically (the pyarrow
advisory explicitly states the vulnerable code path "is not exposed in
language bindings" i.e. not reachable from Python; pytest's issue is a
dev-only local-user attack vector, never shipped to production) — but
both had safe, drop-in fix versions available, so both were upgraded
rather than accepted as risk. **The full 144-test suite was rerun after
both upgrades and passed with zero regressions** — a patch is only
actually safe once verified against real tests, not assumed safe from
reading a changelog. Re-scanned after the upgrade: **0 known
vulnerabilities**.

```bash
pip-audit -r requirements.txt --desc
```

## 4. Non-Root Containers

`infra/docker/Dockerfile.api` now creates and switches to a dedicated
unprivileged user (`useradd ... finguard; USER finguard`) before running
the application — found running as root with no actual need to (no
privileged port binding, no host-filesystem access outside its own
working directory) during this phase's review. The official `postgres:16`
and `apache/kafka` images already run as non-root by default; not
independently re-verified here since Docker itself remains unavailable in
this sandbox (unchanged limitation since Phase 2) — documented as an
assumption based on those images' well-known defaults, not confirmed by
inspection.

## 5. API Authentication

Implemented via `src/api/auth.py` — a single shared API key (`X-API-Key`
header), applied to every endpoint that reads or writes customer/
transaction/financial data (`/score-transaction`, `/customers/{id}/risk`,
`/transactions/{id}`, `/alerts`). **Deliberately left public**:
`GET /health` (monitoring/orchestration systems and Docker healthchecks
need to reach it without a secret) and `GET /metrics` (Prometheus
scraping typically doesn't send custom auth headers by default; a real
deployment would instead firewall the `/metrics` port at the network
level rather than add API-key friction to a scrape target, a more
standard pattern for that specific endpoint).

**Production target**: OAuth2/JWT or an API gateway (Azure API Management,
matching this project's Azure target architecture) — a single shared key
is a reasonable, honestly-scoped local/demo mechanism, not what a real
multi-client production API should use long-term. Documented, not
disguised as more than it is.

Verified with 14 real tests (`tests/unit/test_api_auth.py`,
`tests/integration/test_api.py`) covering: no key configured (allowed
through, warning logged), missing header when a key IS configured (401),
wrong header (401), correct header (200), empty-string header specifically
(401, not accidentally treated as a match), and `/health` remaining
reachable with no key even when one is configured.

## 6. Least-Privilege Review (Postgres)

**Real finding**: `\du finguard` shows the application's Postgres role
currently has `CREATEDB` — broader than the running application needs (it
only performs DML — `SELECT`/`INSERT`/`UPDATE` — on its own tables, never
creates databases). This privilege was granted in Phase 6, for local-dev
convenience, so the same role could create the separate `airflow`
metadata database without provisioning a second Postgres user. **Accepted
as a documented local-dev trade-off, not silently left unexamined**:

- **Local (this repo)**: one `finguard` role, `CREATEDB` granted, used
  for both the application and ad-hoc local database creation. Simpler
  for a single-developer sandbox; the accepted risk is scoped to a local
  Postgres instance with no other tenants.
- **Production target**: a dedicated, narrowly-scoped application role
  (no `CREATEDB`, `GRANT`ed only `SELECT`/`INSERT`/`UPDATE` on the
  specific tables/schemas the app touches — `public`, `public_marts`) and
  a SEPARATE migration/admin role (used only by `dbt`/Airflow/deployment
  tooling, never by the running API or streaming services) for anything
  requiring DDL or database-creation rights. This is standard
  least-privilege practice and should be implemented before any real
  deployment — flagged here as a known gap, not deferred silently.

## 7. Input Validation (Audit, Not New Work)

Reviewed rather than rebuilt: every external input boundary already
validates through Pydantic (`ScoreTransactionRequest` and friends since
Phase 7) or parameterized SQL (`psycopg` placeholders throughout
`src/api/repository.py`, `src/ml/serving.py` — confirmed no f-string SQL
interpolation of user-supplied values anywhere in the codebase except the
one deliberately-reviewed case in `src/ml/serving.py::_load_reference_row`,
where the table/column names are internal constants and only the
parameterized value is user-supplied). This phase's contribution here is
the audit confirming that discipline held across 8 prior phases, not new
validation code.

## 8. Known Limitations

- API key auth is a single shared secret, not per-client credentials or
  OAuth2/JWT — documented production target, not built here.
- Non-root Docker container written but never build-tested (no Docker
  daemon).
- Postgres least-privilege fix (separate app vs. migration roles) is
  documented, not implemented — would require re-provisioning the local
  Postgres setup from earlier phases, out of scope for this pass without
  breaking those phases' already-verified state.
- No TLS between the API and its clients locally (plain HTTP) — matches
  the same "local unauthenticated/unencrypted, TLS+SASL+ACLs in
  production" posture already established for Kafka.
- Rate limiting remains unimplemented (documented gap since Phase 7).

## 9. Addendum (Phase 11): A Third CVE Found During Final Regression

While running the final full regression pass in Phase 11, `pip-audit`
surfaced a finding Phase 9's original scan didn't have: 3 CVEs in
`cryptography` (a transitive dependency of `mlflow`/`psycopg`, never
explicitly pinned in `requirements.txt` until now). This wasn't a
regression in the strict sense — nothing in this project changed to
introduce it — but a new advisory publication combined with the fact that
an unpinned transitive dependency isn't reproducibly scanned run to run
(pip-audit resolves it fresh each time against whatever the currently
pinned direct dependencies allow).

**Investigated and largely fixed**:
- Upgraded `mlflow` 3.14.0 → 3.15.1 specifically to relax its
  `cryptography<49` constraint to `cryptography<50`.
- Pinned `cryptography==49.0.0` explicitly (previously unpinned/transitive) —
  fixes 2 of the 3 CVEs (CVE-2026-69248, a certificate name-constraint
  bypass; CVE-2026-69249, a chain-validation DoS amplification).
- Re-verified the full 151-test suite and a fresh-venv install after both
  changes — zero regressions.

**One CVE remains, as a documented accepted risk**: `PYSEC-2026-3552`
(an RSA/PKCS7 timing oracle in S/MIME decryption), fixed only in
`cryptography==50.0.0`, which mlflow 3.15.1 (the latest available version
at the time of this scan) still doesn't permit. **Verified, not assumed**,
that this project has zero exposure to the vulnerable code path: a direct
grep across the entire codebase (`grep -rniE "pkcs7|smime|s/mime" src/
dags/ dbt/ scripts/`) confirms no PKCS7 or S/MIME decryption occurs
anywhere in this application. The dependency is present only because
`mlflow`/`psycopg` pull in `cryptography` for unrelated TLS/certificate
functionality this project also doesn't exercise directly.

**Lesson for future phases**: an unpinned transitive dependency is
invisible to a point-in-time audit in a way a direct pin isn't — Phase 9's
"0 known vulnerabilities" was accurate for what `pip-audit` could see
*then*, against the dependency tree as resolved *then*, not a permanent
guarantee. `cryptography` is now an explicit pin specifically so this
dependency's CVE status is visible and trackable going forward, not
rediscovered by accident during an unrelated regression pass.
