variable "key_vault_name" {
  description = "Globally unique Key Vault name."
  type        = string
}

variable "location" {
  type = string
}

variable "resource_group_name" {
  type = string
}

variable "app_managed_identity_principal_id" {
  description = "Object ID of the application's managed identity, granted read-only secret access."
  type        = string
}

variable "postgres_password" {
  description = "Stored as a Key Vault secret, never in Terraform state in plaintext logs -- mark sensitive."
  type        = string
  sensitive   = true
}

variable "api_key" {
  description = "The API key src/api/auth.py checks (SECURITY_API_KEY locally) -- see docs/security.md §5."
  type        = string
  sensitive   = true
}

variable "tags" {
  type    = map(string)
  default = {}
}
