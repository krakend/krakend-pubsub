package pubsub

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"

	"gocloud.dev/pubsub"
	_ "gocloud.dev/pubsub/awssnssqs"
	_ "gocloud.dev/pubsub/azuresb"
	_ "gocloud.dev/pubsub/gcppubsub"
	_ "gocloud.dev/pubsub/kafkapubsub"
	_ "gocloud.dev/pubsub/natspubsub"
	_ "gocloud.dev/pubsub/rabbitpubsub"

	"github.com/devopsfaith/krakend/config"
	"github.com/devopsfaith/krakend/logging"
	"github.com/devopsfaith/krakend/proxy"
)

var OpenCensusViews = pubsub.OpenCensusViews
var errNoBackendHostDefined = fmt.Errorf("no host backend defined")

const (
	publisherNamespace  = "github.com/devopsfaith/krakend-pubsub/publisher"
	subscriberNamespace = "github.com/devopsfaith/krakend-pubsub/subscriber"
)

func NewBackendFactory(ctx context.Context, logger logging.Logger, bf proxy.BackendFactory) *BackendFactory {
	return &BackendFactory{
		logger: logger,
		bf:     bf,
		ctx:    ctx,
	}
}

type BackendFactory struct {
	ctx    context.Context
	logger logging.Logger
	bf     proxy.BackendFactory
}

func (f *BackendFactory) New(remote *config.Backend) proxy.Proxy {
	if prxy, err := f.initSubscriber(f.ctx, remote); err == nil {
		return prxy
	}

	if prxy, err := f.initPublisher(f.ctx, remote); err == nil {
		return prxy
	}

	return f.bf(remote)
}

func (f *BackendFactory) initPublisher(ctx context.Context, remote *config.Backend) (proxy.Proxy, error) {
	if len(remote.Host) < 1 {
		return proxy.NoopProxy, errNoBackendHostDefined
	}

	dns := remote.Host[0]
	cfg := &publisherCfg{}
	if err := getConfig(remote, publisherNamespace, cfg); err != nil {
		f.logger.Debug(fmt.Sprintf("pubsub: publisher (%s): %s", dns, err.Error()))
		return proxy.NoopProxy, err
	}

	t, err := pubsub.OpenTopic(ctx, dns+cfg.TopicURL)
	if err != nil {
		f.logger.Error(fmt.Sprintf("pubsub: %s", err.Error()))
		return proxy.NoopProxy, err
	}

	go func() {
		<-ctx.Done()
		t.Shutdown(context.Background())
	}()

	return func(ctx context.Context, r *proxy.Request) (*proxy.Response, error) {
		body, err := ioutil.ReadAll(r.Body)
		if err != nil {
			return nil, err
		}
		headers := map[string]string{}
		for k, vs := range r.Headers {
			headers[k] = vs[0]
		}
		msg := &pubsub.Message{
			Metadata: headers,
			Body:     body,
		}

		if err := t.Send(ctx, msg); err != nil {
			return nil, err
		}
		return &proxy.Response{IsComplete: true}, nil
	}, nil
}

func (f *BackendFactory) initSubscriber(ctx context.Context, remote *config.Backend) (proxy.Proxy, error) {
	if len(remote.Host) < 1 {
		return proxy.NoopProxy, errNoBackendHostDefined
	}

	dns := remote.Host[0]
	cfg := &subscriberCfg{}
	if err := getConfig(remote, subscriberNamespace, cfg); err != nil {
		f.logger.Debug(fmt.Sprintf("pubsub: subscriber (%s): %s", dns, err.Error()))
		return proxy.NoopProxy, err
	}

	topicURL := dns + cfg.SubscriptionURL

	sub, err := pubsub.OpenSubscription(ctx, topicURL)
	if err != nil {
		f.logger.Error(fmt.Sprintf("pubsub: opening subscription for %s: %s", topicURL, err.Error()))
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

type publisherCfg struct {
	TopicURL string `json:"topic_url"`
}

type subscriberCfg struct {
	SubscriptionURL string `json:"subscription_url"`
}

func getConfig(remote *config.Backend, namespace string, v interface{}) error {
	cfg, ok := remote.ExtraConfig[namespace]
	if !ok {
		return &NamespaceNotFoundErr{
			Namespace: namespace,
		}
	}

	b, err := json.Marshal(&cfg)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, &v)
}

type NamespaceNotFoundErr struct {
	Namespace string
}

func (n *NamespaceNotFoundErr) Error() string {
	return n.Namespace + " not found in the extra config"
}
