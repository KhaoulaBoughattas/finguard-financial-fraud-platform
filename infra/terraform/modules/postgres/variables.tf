variable "server_name" {
  description = "Globally unique Postgres Flexible Server name."
  type        = string
}

variable "resource_group_name" {
  type = string
}

variable "location" {
  type = string
}

variable "sku_name" {
  description = "B_Standard_B1ms for dev/cost-conscious; GP_Standard_D2ds_v4 or larger for production -- see docs/terraform.md's cost section."
  type        = string
  default     = "B_Standard_B1ms"
}

variable "storage_mb" {
  type    = number
  default = 32768
}

variable "admin_login" {
  type = string
}

variable "admin_password" {
  description = "Provisioned into Key Vault by the key_vault module, referenced here -- never a literal in this file."
  type        = string
  sensitive   = true
}

variable "backup_retention_days" {
  type    = number
  default = 7
}

variable "tags" {
  type    = map(string)
  default = {}
}
