variable "registry_name" {
  description = "Globally unique registry name (alphanumeric only)."
  type        = string
}

variable "resource_group_name" {
  type = string
}

variable "location" {
  type = string
}

variable "sku" {
  description = "Basic for dev/cost-conscious, Premium for production (private endpoints, geo-replication)."
  type        = string
  default     = "Basic"
}

variable "app_managed_identity_principal_id" {
  description = "Object ID of the identity that pulls the API image at deploy time."
  type        = string
}

variable "tags" {
  type    = map(string)
  default = {}
}
