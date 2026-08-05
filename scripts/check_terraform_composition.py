"""Cross-checks the dev environment's module composition against each
module's own variable/output declarations -- catches real composition
mistakes (a missing required variable, an undeclared variable being
passed, an output reference to a name the module doesn't actually
output) that terraform-config-inspect's per-directory parsing can't see,
since it never resolves cross-module references.

Not a replacement for `terraform validate` (which additionally checks
provider-schema-level correctness, e.g. that `sku_name` is a value the
azurerm_postgresql_flexible_server resource actually accepts) -- that
needs the provider plugin from registry.terraform.io, unreachable in
this sandbox (see docs/terraform.md §2). This script is the best
available structural check without it.
"""
from __future__ import annotations

import re
import sys

import hcl2

REPO_ROOT = "infra/terraform"
IGNORE_KEYS = {"__is_block__", "__comments__"}


def check_variable_composition() -> list[str]:
    problems = []
    with open(f"{REPO_ROOT}/environments/dev/main.tf") as f:
        dev_main = hcl2.load(f)

    for module_block in dev_main.get("module", []):
        for mod_name_raw, mod_config in module_block.items():
            mod_name = mod_name_raw.strip('"')
            source = mod_config["source"].strip('"')
            passed_vars = {k for k in mod_config if k not in IGNORE_KEYS} - {"source"}
            mod_dir_name = source.split("/")[-1]
            mod_path = f"{REPO_ROOT}/modules/{mod_dir_name}/variables.tf"
            with open(mod_path) as vf:
                var_defs = hcl2.load(vf)

            declared_vars: dict[str, bool] = {}
            for vb in var_defs.get("variable", []):
                for vname_raw, vconf in vb.items():
                    declared_vars[vname_raw.strip('"')] = "default" not in vconf

            missing = [v for v, req in declared_vars.items() if req and v not in passed_vars]
            unknown = [v for v in passed_vars if v not in declared_vars]
            if missing:
                problems.append(f"{mod_name}: MISSING required vars: {missing}")
            if unknown:
                problems.append(f"{mod_name}: passing UNDECLARED vars: {unknown}")
    return problems


def check_output_references() -> list[str]:
    problems = []
    with open(f"{REPO_ROOT}/environments/dev/outputs.tf") as f:
        content = f.read()
    refs = re.findall(r"module\.(\w+)\.(\w+)", content)
    for mod_name, output_name in refs:
        out_path = f"{REPO_ROOT}/modules/{mod_name}/outputs.tf"
        try:
            with open(out_path) as of:
                out_defs = hcl2.load(of)
        except FileNotFoundError:
            problems.append(f"module.{mod_name}.{output_name}: no such module directory")
            continue
        declared = {o.strip('"') for ob in out_defs.get("output", []) for o in ob}
        if output_name not in declared:
            problems.append(f"module.{mod_name}.{output_name}: not declared in {out_path}")
    return problems


def main() -> None:
    problems = check_variable_composition() + check_output_references()
    if problems:
        print("COMPOSITION PROBLEMS FOUND:")
        for p in problems:
            print(" -", p)
        sys.exit(1)
    print("Module composition valid: all variables/outputs referenced correctly.")


if __name__ == "__main__":
    main()
