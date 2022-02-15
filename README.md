# krakend-pubsub
A pubsub backend for KrakenD

## Backends

- AWS SNS (Simple Notification Service) and SQS (Simple Queueing Service)
- Azure Service Bus Topic and Subscription
- GCP PubSub
- NATS.io
- RabbitMQ

## Configuration

Just add the extra config at your backend:

```
"backend/pubsub/subscriber": {
	"subscription_url": "gcppubsub://project/topic"
}
```
```
"backend/pubsub/publisher": {
	"topic_url": "gcppubsub://project/topic"
}
```
See the [documentation](https://www.krakend.io/docs/backends/pubsub/)