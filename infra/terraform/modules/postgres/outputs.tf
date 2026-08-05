output "server_id" {
  value = azurerm_postgresql_flexible_server.this.id
}

output "fqdn" {
  value = azurerm_postgresql_flexible_server.this.fqdn
}

output "finguard_database_name" {
  value = azurerm_postgresql_flexible_server_database.finguard.name
}

output "airflow_database_name" {
  value = azurerm_postgresql_flexible_server_database.airflow.name
}
