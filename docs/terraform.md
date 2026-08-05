# Terraform / Azure Deployment Architecture (Phase 10)

## 1. What was actually validated vs. documented

```text
HCL syntax validation (terraform-config-inspect + python-hcl2):  RUN — 9/9 directories valid
Cross-module composition check (variables/outputs):               RUN — 0 problems found
terraform init / plan / apply:                                    NOT RUN — registry.terraform.io
                                                                    and releases.hashicorp.com both
                                                                    return 403 host_not_allowed
```

Confirmed directly, not assumed:
```bash
curl -sI https://releases.hashicorp.com/terraform/   # 403 host_not_allowed
curl -sI https://registry.terraform.io/                # 403 host_not_allowed
```
Same class of limitation as Maven Central (Phase 3, Delta) and Docker
Hub/daemon (Phase 2 onward) — the `terraform` binary itself, and the
`azurerm` provider plugin `terraform init` would need, are both
unreachable. This is a genuinely different flavor of "NOT RUN" than
prior phases, though: instead of having *zero* tooling to validate
against, `terraform-config-inspect` (HashiCorp's own shallow HCL parser,
apt-installable, no registry needed — it's what `terraform`
uses internally for module introspection) let this phase do real,
falsifiable syntax validation, plus a custom cross-module composition
check that caught the class of mistake `terraform-config-inspect` alone
can't see (see §2).

## 2. Validation Methodology

Two independent tools, neither requiring registry access:

1. **`terraform-config-inspect --json <dir>`** — parses each module's HCL
   using HashiCorp's real grammar, reports the resources/variables/
   outputs/module-calls it finds and any `diagnostics` (syntax errors).
   Run against all 8 modules + the `dev` environment: **0 diagnostics**.
2. **`python-hcl2`** — an independent, non-HashiCorp HCL2 parser, run
   against every individual `.tf` file, so a bug specific to one parser's
   grammar implementation doesn't produce a false "valid" result from
   relying on a single tool.

Both wrapped in `scripts/validate_terraform.sh` — run it yourself with:
```bash
bash scripts/validate_terraform.sh
```

**A third, custom check** (`scripts/check_terraform_composition.py`) goes
beyond syntax: it parses `environments/dev/main.tf`'s module blocks and
cross-references every variable passed against what each target module
actually declares (catching missing required variables or undeclared
extras), and every `module.X.Y` output reference in `outputs.tf` against
what module `X` actually outputs. Neither `terraform-config-inspect` nor
`python-hcl2` check this — they parse one file/directory at a time and
never resolve cross-module references. **This is real signal, not
theoretical**: `terraform init`+`validate` would catch the same class of
mistake, and running this custom check first is what a careful engineer
without registry access would actually do — not skip validation
entirely just because the "real" tool is unreachable.

```bash
python3 scripts/check_terraform_composition.py
# Module composition valid: all variables/outputs referenced correctly.
```

**What neither tool checks** (and what only `terraform validate` with
real provider schemas would): whether `sku_name = "B_Standard_B1ms"` is
an actual valid SKU for `azurerm_postgresql_flexible_server`, whether
`azurerm_databricks_workspace`'s `custom_parameters` block accepts
exactly the arguments used, and similar provider-schema-level facts. This
is an honest, named gap — see §7.

## 3. Modules

| Module | Provisions | Maps to (local component) |
|---|---|---|
| `resource_group` | Resource group | — |
| `networking` | VNet, Databricks-delegated subnets, services subnet, deny-by-default NSG | — |
| `storage` | ADLS Gen2 (bronze/silver/gold/checkpoints filesystems) | `DeltaSettings` (Phase 3) |
| `key_vault` | Key Vault, read-only app access policy, Postgres/API-key secrets | `.env`/`SecuritySettings` (Phase 9) |
| `container_registry` | ACR, admin disabled, managed-identity pull | `infra/docker/Dockerfile.api` (Phase 8/9) |
| `postgres` | Postgres Flexible Server, `finguard` + `airflow` databases | `PostgresSettings` (Phase 1) |
| `databricks` | VNet-injected Databricks workspace | Spark/Delta pipeline (Phase 3) |
| `monitoring` | Log Analytics, Application Insights, Azure Monitor managed Prometheus | Prometheus/Grafana (Phase 8) |

`environments/dev/main.tf` composes all eight. No `environments/prod`
directory was built — see §6.

## 4. Secrets in Terraform Itself

`postgres_admin_password` and `api_key` are `sensitive = true` variables
with **no default** — `main.tf` never contains a literal secret value.
The intended flow: `TF_VAR_postgres_admin_password=... terraform apply`
or a `.tfvars` file excluded from version control, matching the same
"secrets never in the codebase" discipline verified by the Phase 9
secrets grep (`docs/security.md §2`). Once applied, those values live in
Key Vault (`key_vault` module) — Terraform state itself still contains
them (a well-known Terraform limitation, not specific to this project),
which is why the commented-out `azurerm` remote-state backend in
`providers.tf` matters for a real deployment: local `terraform.tfstate`
files should never be the actual storage location for state containing
secrets.

## 5. Least-Privilege, Carried Through From Phase 9

The `postgres` module is the actual fix for the gap Phase 9 found but
deliberately didn't fix locally (`docs/security.md §6`: the local
`finguard` role has `CREATEDB`, broader than the running app needs). This
module provisions the `finguard` and `airflow` databases on a fresh
server, which is exactly the right place to provision separate
least-privilege roles from the start — a fresh Terraform-managed server,
unlike retrofitting the already-running, already-tested local sandbox
Postgres instance that eight prior phases depend on. (The actual
role-creation SQL isn't Terraform's job — `azurerm_postgresql_flexible_server`
provisions the server and databases; role/grant statements would run via
a `postgresql` Terraform provider or a post-provisioning script, not
included here to avoid adding a second provider dependency for a single
SQL statement.)

Every module additionally sets `public_network_access_enabled = false`
(storage, Key Vault, Postgres) or `no_public_ip = true` (Databricks) —
consistent, deliberate network-level least-privilege, not just an
application-level policy.

## 6. Why No `environments/prod`

Building a second, mostly-duplicate environment directory (larger SKUs,
`GRS` replication, longer log retention) with no way to validate either
one against real provider schemas would double the surface for an
unverified mistake without doubling any actual verification. `dev`'s
module composition is checked for real (§2); a hypothetical `prod`
directory would only be checked for HCL syntax, since the SKU/size
differences are exactly the kind of provider-schema-level detail neither
available tool validates. The dev/prod distinction is captured instead as
inline comments on the size/SKU/replication variables throughout the
modules (e.g. `storage/variables.tf`: "LRS for dev/cost-conscious, GRS/ZRS
for production durability") — real content, without a second
directory's worth of unverifiable duplication.

## 7. Known Limitations

- `terraform init`/`plan`/`apply` never ran — no registry access. HCL
  syntax and cross-module composition ARE verified; provider-schema-level
  correctness (exact valid values for `sku_name`, `sku`, block arguments)
  is NOT.
- No `environments/prod` directory — see §6 for why.
- Role/grant SQL for the Postgres least-privilege split (§5) isn't
  implemented in Terraform — would need a `postgresql` provider or a
  post-apply script, not built here.
- No CI pipeline running `scripts/validate_terraform.sh` automatically —
  a real deployment should wire this (and, where registry access exists,
  real `terraform validate`) into GitHub Actions (Phase 0's documented
  CI/CD plan, not yet a phase in this project's sequence).
- Cost estimates are not provided — Phase 0 explicitly warns against
  inventing unverified Azure costs; SKU choices are commented with
  dev-vs-production intent, not priced.
