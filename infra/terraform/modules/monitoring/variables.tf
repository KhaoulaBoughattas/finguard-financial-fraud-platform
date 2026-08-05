variable "log_analytics_workspace_name" {
  type = string
}

variable "app_insights_name" {
  type = string
}

variable "monitor_workspace_name" {
  type = string
}

variable "resource_group_name" {
  type = string
}

variable "location" {
  type = string
}

variable "retention_in_days" {
  description = "30 for dev/cost-conscious; longer for production compliance needs."
  type        = number
  default     = 30
}

variable "tags" {
  type    = map(string)
  default = {}
}
