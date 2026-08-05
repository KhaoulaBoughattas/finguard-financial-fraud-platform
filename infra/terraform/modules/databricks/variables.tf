variable "workspace_name" {
  type = string
}

variable "resource_group_name" {
  type = string
}

variable "location" {
  type = string
}

variable "sku" {
  description = "standard for dev/cost-conscious; premium for production (needed for credential passthrough / table ACLs matching least-privilege data access)."
  type        = string
  default     = "standard"
}

variable "vnet_id" {
  type = string
}

variable "public_subnet_name" {
  type = string
}

variable "private_subnet_name" {
  type = string
}

variable "tags" {
  type    = map(string)
  default = {}
}
