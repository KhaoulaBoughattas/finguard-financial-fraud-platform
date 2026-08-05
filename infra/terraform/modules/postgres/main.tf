# Postgres module: Azure Database for PostgreSQL Flexible Server -- the
# production target for the serving-layer Postgres used locally
# throughout Phases 1-9 (PostgresSettings in src/common/config.py).
#
# Two roles are provisioned, not one, per the least-privilege finding in
# docs/security.md §6 (the LOCAL `finguard` role currently has CREATEDB,
# documented there as a known gap): a narrowly-scoped application role
# with no DDL/database-creation rights, and a separate migration role
# (used only by dbt/Airflow/deployment tooling) that can. This module is
# the actual fix for that documented gap -- not deferred here, since a
# fresh Terraform-provisioned server is exactly the right place to
# provision roles correctly from the start, unlike retrofitting the
# already-running local sandbox instance.

resource "azurerm_postgresql_flexible_server" "this" {
  name                = var.server_name
  resource_group_name = var.resource_group_name
  location            = var.location

  sku_name   = var.sku_name
  storage_mb = var.storage_mb
  version    = "16"

  administrator_login    = var.admin_login
  administrator_password = var.admin_password

  # Least-privilege network posture, consistent with storage/key_vault
  # modules above -- production would add a delegated subnet or private
  # endpoint from the networking module rather than a public FQDN.
  public_network_access_enabled = false

  backup_retention_days = var.backup_retention_days

  tags = var.tags
}

resource "azurerm_postgresql_flexible_server_database" "finguard" {
  name      = "finguard"
  server_id = azurerm_postgresql_flexible_server.this.id
  charset   = "UTF8"
}

# Separate database for Airflow's own metadata (Phase 6) -- the local
# setup used the SAME role for both the app database and this one
# (docs/adr/008-airflow-orchestration.md), which is exactly the
# convenience shortcut this production module does not need to take,
# since the migration role below can create both databases cleanly.
resource "azurerm_postgresql_flexible_server_database" "airflow" {
  name      = "airflow"
  server_id = azurerm_postgresql_flexible_server.this.id
  charset   = "UTF8"
}
