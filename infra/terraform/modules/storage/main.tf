# Storage module: ADLS Gen2 account with the Bronze/Silver/Gold filesystem
# containers, matching Phase 3's Delta Lake architecture
# (docs/spark-streaming.md) -- `DeltaSettings.lake_root` locally is
# `data/delta`; in production this account is what that setting would
# point at instead (`abfss://...`).

resource "azurerm_storage_account" "this" {
  name                     = var.storage_account_name
  resource_group_name      = var.resource_group_name
  location                 = var.location
  account_tier             = "Standard"
  account_replication_type = var.replication_type
  account_kind             = "StorageV2"
  is_hns_enabled           = true # hierarchical namespace -- this is what makes it ADLS Gen2, not plain blob storage

  # Least-privilege network posture (docs/security.md §6): deny public
  # network access by default; production would add explicit private
  # endpoint / VNet service-endpoint rules for the Databricks and API
  # subnets from the networking module, not documented further here since
  # the exact rule set depends on which services actually need access.
  public_network_access_enabled = false

  min_tls_version = "TLS1_2"

  tags = var.tags
}

resource "azurerm_storage_data_lake_gen2_filesystem" "bronze" {
  name               = "bronze"
  storage_account_id = azurerm_storage_account.this.id
}

resource "azurerm_storage_data_lake_gen2_filesystem" "silver" {
  name               = "silver"
  storage_account_id = azurerm_storage_account.this.id
}

resource "azurerm_storage_data_lake_gen2_filesystem" "gold" {
  name               = "gold"
  storage_account_id = azurerm_storage_account.this.id
}

# Checkpoint location -- mirrors DeltaSettings.checkpoint_root locally
# (data/checkpoints), see docs/spark-streaming.md §6 for why Spark
# Structured Streaming checkpoints need durable, persistent storage.
resource "azurerm_storage_data_lake_gen2_filesystem" "checkpoints" {
  name               = "checkpoints"
  storage_account_id = azurerm_storage_account.this.id
}
