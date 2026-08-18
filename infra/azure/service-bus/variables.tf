variable "subscription_id" {
  description = "Azure subscription that will contain the development resources."
  type        = string
  nullable    = false

  validation {
    condition = can(regex(
      "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$",
      var.subscription_id
    ))
    error_message = "subscription_id must be an Azure subscription UUID."
  }
}

variable "location" {
  description = "Azure region in which to create the development resources."
  type        = string
  nullable    = false

  validation {
    condition     = length(trimspace(var.location)) > 0
    error_message = "location must not be empty."
  }
}

variable "service_bus_namespace_name" {
  description = "Globally unique name for the development Service Bus namespace."
  type        = string
  nullable    = false

  validation {
    condition = can(regex(
      "^[a-z][a-z0-9-]{4,48}[a-z0-9]$",
      var.service_bus_namespace_name
    ))
    error_message = "service_bus_namespace_name must be 6-50 lowercase letters, numbers, or hyphens; start with a letter; and end with a letter or number."
  }
}

variable "developer_principal_id" {
  description = "Microsoft Entra object ID that receives narrowly scoped development access."
  type        = string
  nullable    = false

  validation {
    condition = can(regex(
      "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$",
      var.developer_principal_id
    ))
    error_message = "developer_principal_id must be a Microsoft Entra object UUID."
  }
}

variable "allowed_ip_cidr" {
  description = "Current developer public IPv4 address in /32 CIDR form."
  type        = string
  nullable    = false

  validation {
    condition = (
      can(cidrnetmask(var.allowed_ip_cidr))
      && endswith(var.allowed_ip_cidr, "/32")
    )
    error_message = "allowed_ip_cidr must be one valid IPv4 address with a /32 suffix."
  }
}
