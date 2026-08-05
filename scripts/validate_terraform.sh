#!/bin/bash
# Validates Terraform HCL syntax without needing the terraform binary or
# registry access (both blocked in the sandbox this was built in -- see
# docs/terraform.md §2). Uses two independent tools:
#   1. terraform-config-inspect (apt package) -- HashiCorp's own shallow
#      HCL parser, used internally by terraform itself for module
#      introspection, so it exercises the real grammar.
#   2. python-hcl2 -- an independent, non-HashiCorp HCL2 parser, so a bug
#      specific to one parser doesn't produce a false "valid" result.
# Neither tool downloads provider plugins or contacts a registry -- both
# are pure syntax/structure checks, not `terraform validate`'s full
# semantic check (which needs provider schemas).
set -e
FAILED=0
for dir in infra/terraform/modules/*/ infra/terraform/environments/*/; do
    name=$(basename "$dir")
    echo "=== $name ==="
    if ! terraform-config-inspect --json "$dir" > /tmp/tci_out.json 2>/tmp/tci_err.txt; then
        echo "FAILED (terraform-config-inspect): $(cat /tmp/tci_err.txt)"
        FAILED=1
        continue
    fi
    if python3 -c "import json; d = json.load(open('/tmp/tci_out.json')); assert not d.get('diagnostics'), d['diagnostics']" 2>/tmp/tci_diag_err.txt; then
        echo "terraform-config-inspect: OK"
    else
        echo "FAILED (diagnostics present): $(cat /tmp/tci_diag_err.txt)"
        FAILED=1
    fi
    for f in "$dir"*.tf; do
        [ -f "$f" ] || continue
        if python3 -c "import hcl2; hcl2.load(open('$f'))" 2>/tmp/hcl2_err.txt; then
            :
        else
            echo "FAILED (python-hcl2) on $f: $(cat /tmp/hcl2_err.txt)"
            FAILED=1
        fi
    done
    echo "python-hcl2: OK"
done
if [ "$FAILED" -eq 1 ]; then
    echo "=== ONE OR MORE MODULES FAILED VALIDATION ==="
    exit 1
fi
echo "=== ALL MODULES VALID ==="
