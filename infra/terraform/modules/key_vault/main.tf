# Key Vault module -- the production secrets target documented since
# Phase 9 (docs/security.md §2): application code reads secrets via the
# Key Vault SDK / injected environment at deploy time, never from a
# committed .env file (which remains the correct LOCAL equivalent, not a
# placeholder meant to resemble this).

data "azurerm_client_config" "current" {}

resource "azurerm_key_vault" "this" {
  name                       = var.key_vault_name
  location                   = var.location
  resource_group_name        = var.resource_group_name
  tenant_id                  = data.azurerm_client_config.current.tenant_id
  sku_name                   = "standard"
  purge_protection_enabled   = true
  soft_delete_retention_days = 90

  # Least-privilege network posture (docs/security.md §6): deny public
  # access by default, matching the storage module's posture.
  public_network_access_enabled = false

  tags = var.tags
}

# Least-privilege access policy for the application's own managed
# identity: GET/LIST secrets only -- never Set/Delete/Purge, which would
# let a compromised application instance rewrite or destroy secrets it
# only needs to READ. Matches the separate app-role vs. migration-role
# split documented for Postgres in docs/security.md §6 -- the same
# principle applied to the secrets store itself.
resource "azurerm_key_vault_access_policy" "app_read_only" {
  key_vault_id = azurerm_key_vault.this.id
  tenant_id    = data.azurerm_client_config.current.tenant_id
  object_id    = var.app_managed_identity_principal_id

  secret_permissions = ["Get", "List"]
}

resource "azurerm_key_vault_secret" "postgres_password" {
  name         = "postgres-password"
  value        = var.postgres_password
  key_vault_id = azurerm_key_vault.this.id
}

resource "azurerm_key_vault_secret" "api_key" {
  name         = "finguard-api-key"
  value        = var.api_key
  key_vault_id = azurerm_key_vault.this.id
}
