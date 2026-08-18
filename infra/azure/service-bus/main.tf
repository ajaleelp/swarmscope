locals {
  resource_group_name = "rg-hindsight-dev"

  common_tags = {
    application = "hindsight"
    environment = "development"
    managed_by  = "terraform"
  }
}

resource "azurerm_resource_group" "hindsight" {
  name     = local.resource_group_name
  location = var.location
  tags     = local.common_tags
}

resource "azurerm_servicebus_namespace" "hindsight" {
  name                = var.service_bus_namespace_name
  location            = azurerm_resource_group.hindsight.location
  resource_group_name = azurerm_resource_group.hindsight.name
  sku                 = "Standard"

  local_auth_enabled            = false
  minimum_tls_version           = "1.2"
  public_network_access_enabled = true

  network_rule_set {
    default_action = "Deny"
    ip_rules       = [var.allowed_ip_cidr]
  }

  tags = local.common_tags
}

resource "azurerm_servicebus_topic" "orders" {
  name         = "orders"
  namespace_id = azurerm_servicebus_namespace.hindsight.id

  requires_duplicate_detection            = true
  duplicate_detection_history_time_window = "PT10M"
}

resource "azurerm_servicebus_subscription" "fulfilment" {
  name     = "fulfilment"
  topic_id = azurerm_servicebus_topic.orders.id

  lock_duration                        = "PT1M"
  max_delivery_count                   = 5
  dead_lettering_on_message_expiration = true
}

resource "azurerm_role_assignment" "developer_sends_orders" {
  scope                = azurerm_servicebus_topic.orders.id
  role_definition_name = "Azure Service Bus Data Sender"
  principal_id         = var.developer_principal_id
  principal_type       = "User"
}

resource "azurerm_role_assignment" "developer_receives_fulfilment" {
  scope                = azurerm_servicebus_subscription.fulfilment.id
  role_definition_name = "Azure Service Bus Data Receiver"
  principal_id         = var.developer_principal_id
  principal_type       = "User"
}
