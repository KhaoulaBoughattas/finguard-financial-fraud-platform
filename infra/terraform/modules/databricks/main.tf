# Databricks module -- the production target for Phase 3's Spark
# Structured Streaming + Delta Lake pipeline (docs/spark-streaming.md),
# which ran only against a plain local Spark session in this sandbox
# (Delta's Maven-hosted connector was unreachable -- docs/spark-streaming.md
# §7). VNet-injected into the two delegated subnets from the networking
# module, per Azure's documented requirement for a customer-managed VNet.

resource "azurerm_databricks_workspace" "this" {
  name                = var.workspace_name
  resource_group_name = var.resource_group_name
  location            = var.location
  sku                 = var.sku

  custom_parameters {
    no_public_ip        = true # least-privilege network posture, consistent with every other module
    virtual_network_id  = var.vnet_id
    public_subnet_name  = var.public_subnet_name
    private_subnet_name = var.private_subnet_name
  }

  tags = var.tags
}
