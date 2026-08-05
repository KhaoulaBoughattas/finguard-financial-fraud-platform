variable "name" {
  description = "Resource group name."
  type        = string
}

variable "location" {
  description = "Azure region, e.g. westeurope."
  type        = string
}

variable "tags" {
  description = "Common tags applied to the resource group."
  type        = map(string)
  default     = {}
}
