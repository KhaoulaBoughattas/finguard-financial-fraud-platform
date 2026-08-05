output "resource_group_name" {
  value = module.resource_group.name
}

output "storage_dfs_endpoint" {
  value = module.storage.primary_dfs_endpoint
}

output "container_registry_login_server" {
  value = module.container_registry.login_server
}

output "postgres_fqdn" {
  value = module.postgres.fqdn
}

output "databricks_workspace_url" {
  value = module.databricks.workspace_url
}

output "key_vault_uri" {
  value = module.key_vault.key_vault_uri
}

output "app_insights_connection_string" {
  value     = module.monitoring.app_insights_connection_string
  sensitive = true
}
