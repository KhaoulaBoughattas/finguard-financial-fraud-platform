# Dev environment: composes every module in infra/terraform/modules/ into
# the target architecture documented throughout this project's ADRs
# (docs/adr/003 through docs/adr/011's "Local vs. Production" tables).
# NOT applied in this sandbox -- see providers.tf and docs/terraform.md §2.

module "resource_group" {
  source   = "../../modules/resource_group"
  name     = "${var.name_prefix}-rg"
  location = var.location
  tags     = var.tags
}

module "networking" {
  source              = "../../modules/networking"
  name_prefix         = var.name_prefix
  location            = var.location
  resource_group_name = module.resource_group.name
  tags                = var.tags
}

module "storage" {
  source                = "../../modules/storage"
  storage_account_name  = replace("${var.name_prefix}sa", "-", "")
  resource_group_name   = module.resource_group.name
  location              = var.location
  replication_type      = "LRS" # dev/cost-conscious -- see docs/terraform.md §5
  tags                  = var.tags
}

module "container_registry" {
  source                             = "../../modules/container_registry"
  registry_name                      = replace("${var.name_prefix}acr", "-", "")
  resource_group_name                = module.resource_group.name
  location                           = var.location
  sku                                = "Basic" # dev/cost-conscious
  app_managed_identity_principal_id  = var.app_managed_identity_principal_id
  tags                               = var.tags
}

module "postgres" {
  source               = "../../modules/postgres"
  server_name          = "${var.name_prefix}-postgres"
  resource_group_name  = module.resource_group.name
  location             = var.location
  sku_name             = "B_Standard_B1ms" # dev/cost-conscious -- see docs/terraform.md §5
  admin_login          = var.postgres_admin_login
  admin_password       = var.postgres_admin_password
  tags                 = var.tags
}

module "databricks" {
  source              = "../../modules/databricks"
  workspace_name      = "${var.name_prefix}-databricks"
  resource_group_name = module.resource_group.name
  location            = var.location
  sku                 = "standard" # dev/cost-conscious -- see docs/terraform.md §5
  vnet_id             = module.networking.vnet_id
  public_subnet_name  = "${var.name_prefix}-databricks-public"
  private_subnet_name = "${var.name_prefix}-databricks-private"
  tags                = var.tags
}

module "key_vault" {
  source                             = "../../modules/key_vault"
  key_vault_name                     = "${var.name_prefix}-kv"
  location                           = var.location
  resource_group_name                = module.resource_group.name
  app_managed_identity_principal_id  = var.app_managed_identity_principal_id
  postgres_password                  = var.postgres_admin_password
  api_key                            = var.api_key
  tags                               = var.tags
}

module "monitoring" {
  source                        = "../../modules/monitoring"
  log_analytics_workspace_name = "${var.name_prefix}-logs"
  app_insights_name             = "${var.name_prefix}-insights"
  monitor_workspace_name        = "${var.name_prefix}-prometheus"
  resource_group_name           = module.resource_group.name
  location                      = var.location
  retention_in_days             = 30 # dev/cost-conscious
  tags                           = var.tags
}
