package pubsub

import (
	"bytes"
	"context"
	"encoding/json"
	"io/ioutil"
	"strings"
	"testing"

	"gocloud.dev/pubsub"
	_ "gocloud.dev/pubsub/mempubsub"

	"github.com/devopsfaith/krakend/config"
	"github.com/devopsfaith/krakend/encoding"
	"github.com/devopsfaith/krakend/logging"
	"github.com/devopsfaith/krakend/proxy"
)

func TestNew_noConfig(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	called := false

	fallback := func(remote *config.Backend) proxy.Proxy {
		called = true
		return proxy.NoopProxy
	}

	buff := bytes.NewBuffer(make([]byte, 1024))
	logger, _ := logging.NewLogger("DEBUG", buff, "")

	bf := NewBackendFactory(ctx, logger, fallback)

	prxy := bf.New(&config.Backend{
		Host: []string{"schema://host"},
	})

	prxy(context.Background(), &proxy.Request{})

	if !called {
		t.Error("fallback should be called")
	}

	lines := strings.Split(buff.String(), "\n")
	if !strings.HasSuffix(lines[0], "DEBUG: pubsub: subscriber (schema://host): github.com/devopsfaith/krakend-pubsub/subscriber not found in the extra config") {
		t.Error("unexpected first log line:", lines[0])
	}
	if !strings.HasSuffix(lines[1], "DEBUG: pubsub: publisher (schema://host): github.com/devopsfaith/krakend-pubsub/publisher not found in the extra config") {
		t.Error("unexpected second log line:", lines[1])
	}
	if lines[2] != "" {
		t.Error("unexpected final log line:", lines[2])
	}
}

func TestNew_subscriber(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fallback := func(remote *config.Backend) proxy.Proxy {
		t.Error("fallback shouldn't be called")
		return proxy.NoopProxy
	}

	buff := new(bytes.Buffer)
	logger, _ := logging.NewLogger("DEBUG", buff, "")

	bf := NewBackendFactory(ctx, logger, fallback)

	topic, err := pubsub.OpenTopic(ctx, "mem://host/subscriber-topic-url")

	prxy := bf.New(&config.Backend{
		Host:    []string{"mem://host"},
		Decoder: encoding.JSONDecoder,
		ExtraConfig: config.ExtraConfig{
			subscriberNamespace: &subscriberCfg{
				SubscriptionURL: "/subscriber-topic-url",
			},
		},
	})

	topic.Send(ctx, &pubsub.Message{
		Body: []byte(`{"foobar":42}`),
	})

	resp, err := prxy(context.Background(), &proxy.Request{})

	if err != nil && err != context.Canceled {
		t.Error(err)
	}

	if log := buff.String(); log != "" {
		t.Errorf("unexpected log: '%s'", log)
	}

	if !resp.IsComplete {
		t.Errorf("got an incomplete response")
		return
	}

	v := resp.Data["foobar"]
	foobar, ok := v.(json.Number)
	if !ok {
		t.Errorf("unexpected response: %+v", resp.Data)
		return
	}
	if foobar.String() != "42" {
		t.Errorf("unexpected response: %+v", resp.Data)
		return
	}
}

func TestNew_publisher(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fallback := func(remote *config.Backend) proxy.Proxy {
		t.Error("fallback shouldn't be called")
		return proxy.NoopProxy
	}

	buff := bytes.NewBuffer(make([]byte, 1024))
	logger, _ := logging.NewLogger("DEBUG", buff, "")

	bf := NewBackendFactory(ctx, logger, fallback)

	prxy := bf.New(&config.Backend{
		Host: []string{"mem://host"},
		ExtraConfig: config.ExtraConfig{
			publisherNamespace: &publisherCfg{
				TopicURL: "mem://publisher-topic-url",
			},
		},
	})

	prxy(context.Background(), &proxy.Request{Body: ioutil.NopCloser(bytes.NewBufferString(`{"foo":"bar"}`))})

	lines := strings.Split(buff.String(), "\n")
	if !strings.HasSuffix(lines[0], "DEBUG: pubsub: subscriber (mem://host): github.com/devopsfaith/krakend-pubsub/subscriber not found in the extra config") {
		t.Error("unexpected first log line:", lines[0])
	}
	if lines[1] != "" {
		t.Error("unexpected final log line:", lines[1])
	}
}

func TestNew_publisher_unknownProvider(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	called := false

	fallback := func(remote *config.Backend) proxy.Proxy {
		called = true
		return proxy.NoopProxy
	}

	buff := bytes.NewBuffer(make([]byte, 1024))
	logger, _ := logging.NewLogger("DEBUG", buff, "")

	bf := NewBackendFactory(ctx, logger, fallback)

	prxy := bf.New(&config.Backend{
		Host: []string{"schema://host"},
		ExtraConfig: config.ExtraConfig{
			publisherNamespace: &publisherCfg{
				TopicURL: "schema://publisher-topic-url",
			},
		},
	})

	prxy(context.Background(), &proxy.Request{Body: ioutil.NopCloser(bytes.NewBufferString(`{"foo":"bar"}`))})

	if !called {
		t.Error("fallback should be called")
	}

	lines := strings.Split(buff.String(), "\n")
	if !strings.HasSuffix(lines[0], "DEBUG: pubsub: subscriber (schema://host): github.com/devopsfaith/krakend-pubsub/subscriber not found in the extra config") {
		t.Error("unexpected first log line:", lines[0])
	}
	if !strings.HasSuffix(lines[1], `ERROR: pubsub: open pubsub.Topic: no driver registered for "schema" for URL "schema://publisher-topic-url"; available schemes: awssns, awssqs, azuresb, gcppubsub, kafka, mem, nats, rabbit`) {
		t.Error("unexpected second log line:", lines[1])
	}
	if lines[2] != "" {
		t.Error("unexpected final log line:", lines[2])
	}
}
