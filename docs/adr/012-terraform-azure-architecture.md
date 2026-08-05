# ADR 012: Terraform / Azure Deployment Architecture

## Context
Phase 0 asks for reusable Terraform modules covering the Azure target
architecture (resource group, storage/ADLS, Databricks, Key Vault,
monitoring, networking, container registry), with deployment
documentation and a cost-conscious posture. Phase 10 had to decide how to
validate Terraform in a sandbox where both `releases.hashicorp.com` (the
`terraform` binary itself) and `registry.terraform.io` (the `azurerm`
provider `terraform init` needs) are network-blocked — a strictly harder
version of the Maven Central (Phase 3) and Docker (Phase 2+) limitations,
since even the CLI tool itself couldn't be installed the normal way.

## Decision: validate with terraform-config-inspect + python-hcl2 + a
custom composition checker, not skip validation
`terraform-config-inspect` is apt-installable and is HashiCorp's own
shallow HCL parser (used internally by `terraform` for module
introspection) — genuinely exercises the real grammar without needing
registry access. Paired with `python-hcl2` (an independent, non-HashiCorp
parser) so a single parser's bug can't produce a false "valid" result.
Neither tool resolves cross-module references, so a custom script
(`scripts/check_terraform_composition.py`) was written to catch exactly
that class of mistake: every variable passed into a module call checked
against what the target module actually declares (required-but-missing,
or extra-and-undeclared), and every `module.X.Y` output reference checked
against what module `X` actually outputs. This is real, falsifiable
verification — not equivalent to `terraform validate` (which additionally
checks provider-schema-level correctness), but a meaningfully stronger
check than "the files exist and look plausible."

## Decision: one `environments/dev` directory, not also `prod`
A second, mostly-duplicate environment (larger SKUs, different
replication/retention settings) would only be checked at the same HCL-
syntax level as `dev` — none of the dev/prod-specific values (SKU names,
replication types) are validated against real provider schemas by either
available tool. Rather than build an environment that looks more complete
than it's actually verified to be, dev/prod differences are captured as
inline comments on the relevant variables throughout the modules. This
mirrors the project's standing principle (visible since Phase 4's "no
`environments/prod`-equivalent dbt duplication either") of not adding
structure that can't be backed by real verification.

## Decision: least-privilege Postgres roles are the `postgres` module's
job, fixing (not just documenting) the Phase 9 gap
Phase 9 found that the local `finguard` Postgres role has `CREATEDB` —
broader than needed — and explicitly deferred fixing it locally to avoid
destabilizing already-verified earlier-phase state
(`docs/adr/011-security-secrets-management.md`). A fresh
Terraform-provisioned server is exactly the right place to provision
separate least-privilege roles from the start, since there's no existing
verified state to risk breaking. The `postgres` module provisions
separate `finguard` and `airflow` databases on one server as the
foundation for that split; the actual role/GRANT SQL is flagged as a
follow-up needing either a `postgresql` Terraform provider or a
post-apply script (not added here, to avoid a second, heavier provider
dependency for one SQL statement worth of work) — the fix is
architected, not fully implemented, and that boundary is stated
explicitly rather than blurred.

## Decision: secrets never appear as literals in any `.tf` file
`postgres_admin_password` and `api_key` are `sensitive = true` variables
with no default, sourced via `TF_VAR_*` environment variables or a
gitignored `.tfvars` file — consistent with the Phase 9 secrets-grep
discipline extended to infrastructure-as-code, not just application code.
Documented candidly that Terraform *state* itself still contains these
values in plaintext once applied (a well-known Terraform limitation, not
specific to this project) — which is why a real remote-state backend
(commented out in `providers.tf`, not configured with placeholder values)
matters for an actual deployment.

## Local vs. Production
| | Local (this repo) | Production target |
|---|---|---|
| Terraform execution | Never run (`init`/`plan`/`apply`) — no registry access | Real CI pipeline running `terraform plan`/`apply` |
| State storage | N/A (never applied) | Azure Storage backend (commented template in `providers.tf`) |
| Validation | `terraform-config-inspect` + `python-hcl2` + custom composition check | Full `terraform validate`/`plan` against real provider schemas |
| Postgres roles | One role (`finguard`), `CREATEDB` granted (Phase 9 documented gap) | Separate app/migration roles, architected in the `postgres` module |

## Known Limitations
- No `terraform init`/`plan`/`apply` ever ran — provider-schema-level
  correctness (exact valid SKU values, resource argument names) is
  unverified beyond HCL syntax and cross-module composition.
- No `environments/prod` directory (see decision above).
- Postgres role/GRANT SQL for the least-privilege split isn't implemented
  in Terraform yet.
- No CI wiring for `scripts/validate_terraform.sh` — a real deployment
  should add this to the project's eventual CI/CD pipeline (Phase 0's
  documented GitHub Actions plan).

## Consequences
- Any future phase adding a new Azure resource should add a new module
  under `infra/terraform/modules/`, wire it into
  `environments/dev/main.tf`, and rerun both
  `scripts/validate_terraform.sh` and
  `scripts/check_terraform_composition.py` before considering it done —
  the same two-tool-plus-composition-check pattern established here.
- Whoever first runs this against a real Azure subscription should treat
  `terraform plan`'s output as the first real provider-schema validation
  this configuration has ever had, and review it carefully rather than
  assuming the HCL-level validation already performed here is
  equivalent.
