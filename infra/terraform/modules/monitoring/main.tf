# Monitoring module -- Azure Monitor / Log Analytics / Application
# Insights, the production target for Phase 8's Prometheus/Grafana setup
# (docs/observability.md), which in this sandbox only ran the FastAPI
# /metrics endpoint itself -- Prometheus/Grafana containers never started
# (no Docker daemon). On Azure, Application Insights can scrape the SAME
# /metrics endpoint (Azure Monitor supports Prometheus-compatible
# scraping as of recent versions) -- the metrics defined in
# src/api/metrics.py do not need to change to move from local Prometheus
# to this target, only the scrape configuration does.

resource "azurerm_log_analytics_workspace" "this" {
  name                = var.log_analytics_workspace_name
  resource_group_name = var.resource_group_name
  location            = var.location
  sku                 = "PerGB2018"
  retention_in_days   = var.retention_in_days

  tags = var.tags
}

resource "azurerm_application_insights" "this" {
  name                = var.app_insights_name
  resource_group_name = var.resource_group_name
  location            = var.location
  workspace_id        = azurerm_log_analytics_workspace.this.id
  application_type    = "web"

  tags = var.tags
}

# Azure Monitor's managed Prometheus add-on -- scrapes the same
# /metrics endpoint format src/api/metrics.py already exposes and
# tests/integration/test_metrics.py already verifies, without requiring
# a self-managed Prometheus server (the local docker-compose.yml service,
# documented as unexecuted in docs/observability.md §1).
resource "azurerm_monitor_workspace" "prometheus" {
  name                = var.monitor_workspace_name
  resource_group_name = var.resource_group_name
  location            = var.location

  tags = var.tags
}
