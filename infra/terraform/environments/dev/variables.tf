variable "location" {
  description = "Azure region."
  type        = string
  default     = "westeurope"
}

variable "name_prefix" {
  description = "Prefix applied to every resource name in this environment."
  type        = string
  default     = "finguard-dev"
}

variable "postgres_admin_login" {
  type    = string
  default = "finguardadmin"
}

variable "postgres_admin_password" {
  description = "Never set via a literal here -- pass via TF_VAR_postgres_admin_password or a .tfvars file excluded from version control (see docs/terraform.md §4)."
  type        = string
  sensitive   = true
}

variable "api_key" {
  description = "The SECURITY_API_KEY value stored into Key Vault -- same sourcing rule as postgres_admin_password above."
  type        = string
  sensitive   = true
}

variable "app_managed_identity_principal_id" {
  description = "Object ID of the FastAPI service's managed identity (created outside this module set, e.g. by the App Service/Container Apps deployment)."
  type        = string
}

variable "tags" {
  type = map(string)
  default = {
    project     = "finguard-fraud-platform"
    environment = "dev"
    managed_by  = "terraform"
  }
}
