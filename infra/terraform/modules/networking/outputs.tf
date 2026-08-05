output "vnet_id" {
  value = azurerm_virtual_network.this.id
}

output "databricks_public_subnet_id" {
  value = azurerm_subnet.databricks_public.id
}

output "databricks_private_subnet_id" {
  value = azurerm_subnet.databricks_private.id
}

output "services_subnet_id" {
  value = azurerm_subnet.services.id
}
