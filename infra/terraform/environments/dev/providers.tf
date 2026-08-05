# Provider configuration for the dev environment.
#
# NOT applied in this sandbox -- `terraform init` needs to download the
# azurerm provider from registry.terraform.io, which returns
# 403 host_not_allowed under this sandbox's network policy (confirmed
# directly). See docs/terraform.md §2.

terraform {
  required_version = ">= 1.9"

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 4.0"
    }
  }

  # Remote state target (production): Azure Storage backend, matching the
  # storage module's own ADLS Gen2 account pattern -- kept commented out
  # rather than configured with placeholder values, since an incomplete
  # backend block is worse than none (Terraform would refuse to init on
  # a genuinely empty/wrong one anyway, but a placeholder invites someone
  # to copy-paste it unmodified into a real deployment).
  #
  # backend "azurerm" {
  #   resource_group_name  = "finguard-tfstate-rg"
  #   storage_account_name = "finguardtfstate"
  #   container_name       = "tfstate"
  #   key                  = "dev.terraform.tfstate"
  # }
}

provider "azurerm" {
  features {
    key_vault {
      purge_soft_delete_on_destroy = false # never auto-purge secrets on `terraform destroy`
    }
  }
}
