# Notification Service with Kafka and Dead Letter Queue

## Run Kafka

```bash
cd "$(dirname "$0")/../kafka" && docker compose up -d
```

Kafka UI: http://localhost:19000

## Run the Service

```bash
cd ../notification-service
./mvnw spring-boot:run
```

## Send a Notification

```bash
curl -X POST http://localhost:8080/api/notifications \
  -H 'Content-Type: application/json' \
  -d '{
    "recipient": "user@example.com",
    "channel": "EMAIL",
    "subject": "Test",
    "message": "Hello",
    "metadata": {"orderId": "123"}
  }'
```

To test DLQ, set subject including the text "fail".

## Topics

- notification.events
- notification.events-dlt


