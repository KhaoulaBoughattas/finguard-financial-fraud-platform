output "storage_account_id" {
  value = azurerm_storage_account.this.id
}

output "primary_dfs_endpoint" {
  value = azurerm_storage_account.this.primary_dfs_endpoint
}

output "bronze_filesystem_name" {
  value = azurerm_storage_data_lake_gen2_filesystem.bronze.name
}

output "silver_filesystem_name" {
  value = azurerm_storage_data_lake_gen2_filesystem.silver.name
}

output "gold_filesystem_name" {
  value = azurerm_storage_data_lake_gen2_filesystem.gold.name
}
