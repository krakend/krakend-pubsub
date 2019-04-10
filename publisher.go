package pubsub

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"

	"gocloud.dev/pubsub"
	_ "gocloud.dev/pubsub/awssnssqs"
	_ "gocloud.dev/pubsub/azuresb"
	_ "gocloud.dev/pubsub/gcppubsub"
	_ "gocloud.dev/pubsub/natspubsub"
	_ "gocloud.dev/pubsub/rabbitpubsub"

	"github.com/devopsfaith/krakend/config"
	"github.com/devopsfaith/krakend/proxy"
)

const publisherNamespace = "github.com/devopsfaith/krakend-pubsub/publisher"

var errNoPublisherCfgDefined = errors.New("no pubsub publisher defined")

func getPublisherConfig(remote *config.Backend) (*publisherCfg, error) {
	v, ok := remote.ExtraConfig[publisherNamespace]
	if !ok {
		return nil, errNoPublisherCfgDefined
	}

	b, _ := json.Marshal(v)
	cfg := &publisherCfg{}
	err := json.Unmarshal(b, cfg)
	return cfg, err
}

type publisherCfg struct {
	TopicURL string `json:"topic_url"`
}

func (f backendFactory) initPublisher(ctx context.Context, remote *config.Backend) (proxy.Proxy, error) {
	dns := remote.Host[0]

	cfg, err := getPublisherConfig(remote)
	if err != nil {
		f.logger.Debug(fmt.Sprintf("pubsub: %s: %s", dns, err.Error()))
		return proxy.NoopProxy, err
	}

	t, err := pubsub.OpenTopic(ctx, dns+cfg.TopicURL)
	if err != nil {
		f.logger.Error(fmt.Sprintf("pubsub: opening the topic for %s/%s: %s", dns, cfg.TopicURL, err.Error()))
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
