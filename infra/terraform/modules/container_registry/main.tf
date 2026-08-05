# Container Registry module -- stores the FastAPI image built from
# infra/docker/Dockerfile.api (Phase 8/9), which runs as a non-root user
# (docs/security.md §4) but has never actually been built in this
# sandbox (no Docker daemon). Admin user disabled deliberately: pulls
# should authenticate via the deploying service's managed identity
# (least-privilege, matches the Key Vault access-policy pattern above),
# not a shared admin credential.

resource "azurerm_container_registry" "this" {
  name                = var.registry_name
  resource_group_name = var.resource_group_name
  location            = var.location
  sku                 = var.sku
  admin_enabled       = false

  tags = var.tags
}

resource "azurerm_role_assignment" "app_pull" {
  scope                = azurerm_container_registry.this.id
  role_definition_name = "AcrPull"
  principal_id          = var.app_managed_identity_principal_id
}
