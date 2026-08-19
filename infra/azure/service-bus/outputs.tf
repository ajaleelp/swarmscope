output "service_bus_fully_qualified_namespace" {
  description = "Service Bus hostname used by passwordless application clients."

  # Azure returns "https://<host>:443/" for this attribute, but the SDK requires
  # a bare hostname. Match the host regardless of scheme or port so this keeps
  # working if the endpoint format changes.
  value = regex(
    "^(?:[a-z]+://)?([^/:]+)",
    azurerm_servicebus_namespace.swarmscope.endpoint
  )[0]
}

output "orders_topic_name" {
  description = "Topic to which the order publisher sends events."
  value       = azurerm_servicebus_topic.orders.name
}

output "fulfilment_subscription_name" {
  description = "Subscription from which the fulfilment worker receives order events."
  value       = azurerm_servicebus_subscription.fulfilment.name
}
