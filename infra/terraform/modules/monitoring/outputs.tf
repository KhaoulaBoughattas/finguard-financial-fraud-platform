output "log_analytics_workspace_id" {
  value = azurerm_log_analytics_workspace.this.id
}

output "app_insights_connection_string" {
  value     = azurerm_application_insights.this.connection_string
  sensitive = true
}

output "prometheus_workspace_id" {
  value = azurerm_monitor_workspace.prometheus.id
}
