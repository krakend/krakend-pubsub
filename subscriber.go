package pubsub

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"gocloud.dev/pubsub"
	_ "gocloud.dev/pubsub/awssnssqs"
	_ "gocloud.dev/pubsub/azuresb"
	_ "gocloud.dev/pubsub/gcppubsub"
	_ "gocloud.dev/pubsub/natspubsub"
	_ "gocloud.dev/pubsub/rabbitpubsub"

	"github.com/devopsfaith/krakend/config"
	"github.com/devopsfaith/krakend/proxy"
)

const subscriberNamespace = "github.com/devopsfaith/krakend-pubsub/subscriber"

var errNoSubscriberCfgDefined = errors.New("no pubsub subscriber defined")

func getSubscriberConfig(remote *config.Backend) (*subscriberCfg, error) {
	v, ok := remote.ExtraConfig[subscriberNamespace]
	if !ok {
		return nil, errNoSubscriberCfgDefined
	}

	b, _ := json.Marshal(v)
	cfg := &subscriberCfg{}
	err := json.Unmarshal(b, cfg)
	return cfg, err
}

type subscriberCfg struct {
	SubscriptionURL string `json:"subscription_url"`
}

func (f backendFactory) initSubscriber(ctx context.Context, remote *config.Backend) (proxy.Proxy, error) {
	dns := remote.Host[0]

	cfg, err := getSubscriberConfig(remote)
	if err != nil {
		f.logger.Debug(fmt.Sprintf("pubsub: %s: %s", dns, err.Error()))
		return proxy.NoopProxy, err
	}

	sub, err := pubsub.OpenSubscription(ctx, dns+cfg.SubscriptionURL)
	if err != nil {
		f.logger.Error(fmt.Sprintf("pubsub: opening subscription for %s/%s: %s", dns, cfg.SubscriptionURL, err.Error()))
		return proxy.NoopProxy, err
	}

	go func() {
		<-ctx.Done()
		sub.Shutdown(context.Background())
	}()

	ef := proxy.NewEntityFormatter(remote)

	return func(ctx context.Context, _ *proxy.Request) (*proxy.Response, error) {
		msg, err := sub.Receive(ctx)
		if err != nil {
			return nil, err
		}

		var data map[string]interface{}
		if err := remote.Decoder(bytes.NewBuffer(msg.Body), &data); err != nil && err != io.EOF {
			// TODO: figure out how to Nack if possible
			// msg.Nack()
			return nil, err
		}

		msg.Ack()

		newResponse := proxy.Response{Data: data, IsComplete: true}
		newResponse = ef.Format(newResponse)
		return &newResponse, nil
	}, nil
}
