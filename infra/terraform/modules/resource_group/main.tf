# Resource Group module.
#
# NOT applied in this sandbox -- terraform itself, and the AzureRM
# provider it would need from registry.terraform.io, could not be
# installed (both HashiCorp's release server and the Terraform Registry
# return 403 host_not_allowed under this sandbox's network policy,
# confirmed directly with curl -- the same class of limitation as Maven
# Central for Delta in Phase 3 and Docker for Kafka since Phase 2).
# Written to be correct and ready wherever `terraform init`/`apply` can
# actually reach the registry; validated here with `terraform-config-inspect`
# (apt-installable, no registry access needed) and `python-hcl2` for
# real syntax verification -- see docs/terraform.md §2.

resource "azurerm_resource_group" "this" {
  name     = var.name
  location = var.location
  tags     = var.tags
}
