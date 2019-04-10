# krakend-pubsub
a pubsub backend for the KrakenD framework

**NOTICE: this module is experimental and under developement. Expect sub-optimal performance and possible breaking changes**

## Backends

- AWS SNS (Simple Notification Service) and SQS (Simple Queueing Service)
- Azure Service Bus Topic and Subscription
- GCP PubSub
- NATS.io
- RabbitMQ 

## Configuration

Just add the extra config at your backend:

```
"github.com/devopsfaith/krakend-pubsub/subscriber": {
	"subscription_url": "gcppubsub://project/topic"
}
```
```
"github.com/devopsfaith/krakend-pubsub/publisher": {
	"topic_url": "gcppubsub://project/topic"
}
```
