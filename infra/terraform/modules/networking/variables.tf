variable "name_prefix" {
  description = "Prefix applied to all resource names in this module."
  type        = string
}

variable "location" {
  type = string
}

variable "resource_group_name" {
  type = string
}

variable "vnet_cidr" {
  description = "CIDR block for the VNet."
  type        = string
  default     = "10.0.0.0/16"
}

variable "databricks_public_subnet_cidr" {
  type    = string
  default = "10.0.1.0/24"
}

variable "databricks_private_subnet_cidr" {
  type    = string
  default = "10.0.2.0/24"
}

variable "services_subnet_cidr" {
  type    = string
  default = "10.0.3.0/24"
}

variable "tags" {
  type    = map(string)
  default = {}
}
