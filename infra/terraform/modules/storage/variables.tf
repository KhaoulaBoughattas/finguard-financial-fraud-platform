variable "storage_account_name" {
  description = "Globally unique storage account name (lowercase, no hyphens, <=24 chars)."
  type        = string
}

variable "resource_group_name" {
  type = string
}

variable "location" {
  type = string
}

variable "replication_type" {
  description = "LRS for dev/cost-conscious, GRS/ZRS for production durability -- see docs/terraform.md's cost section."
  type        = string
  default     = "LRS"
}

variable "tags" {
  type    = map(string)
  default = {}
}
