output "service_bus_fully_qualified_namespace" {
  description = "Service Bus hostname used by passwordless application clients."

  value = trimsuffix(
    trimprefix(azurerm_servicebus_namespace.swarmscope.endpoint, "sb://"),
    "/"
  )
}

output "orders_topic_name" {
  description = "Topic to which the order publisher sends events."
  value       = azurerm_servicebus_topic.orders.name
}

output "fulfilment_subscription_name" {
  description = "Subscription from which the fulfilment worker receives order events."
  value       = azurerm_servicebus_subscription.fulfilment.name
}
