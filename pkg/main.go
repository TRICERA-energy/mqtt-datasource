package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"time"

	paho "github.com/eclipse/paho.mqtt.golang"
	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/backend/datasource"
	"github.com/grafana/grafana-plugin-sdk-go/backend/instancemgmt"
	"github.com/grafana/grafana-plugin-sdk-go/backend/log"
	"github.com/grafana/grafana-plugin-sdk-go/data"
)

func main() {
	if err := datasource.Manage("grafana-mqtt-datasource", create, datasource.ManageOpts{}); err != nil {
		log.DefaultLogger.Error(err.Error())
		os.Exit(1)
	}
}

func create(s backend.DataSourceInstanceSettings) (instancemgmt.Instance, error) {
	var settings struct {
		Host     string `json:"host"`
		Port     uint16 `json:"port"`
		Username string `json:"username"`
		Password string `json:"password"`
	}
	if err := json.Unmarshal(s.JSONData, &settings); err != nil {
		return nil, err
	}
	if password, exists := s.DecryptedSecureJSONData["password"]; exists {
		settings.Password = password
	}

	client := paho.NewClient(paho.NewClientOptions().
		AddBroker(fmt.Sprintf("tcp://%s:%d", settings.Host, settings.Port)).
		SetClientID(fmt.Sprintf("grafana_%d", rand.Int())).
		SetUsername(settings.Username).
		SetPassword(settings.Password).
		SetPingTimeout(60 * time.Second).
		SetKeepAlive(60 * time.Second).
		SetAutoReconnect(true).
		SetMaxReconnectInterval(10 * time.Second).
		SetConnectionLostHandler(func(_ paho.Client, err error) {
			log.DefaultLogger.Error(fmt.Sprintf("MQTT Connection Lost: %v", err))
		}).
		SetReconnectingHandler(func(_ paho.Client, _ *paho.ClientOptions) {
			log.DefaultLogger.Debug("MQTT Reconnecting")
		}),
	)

	log.DefaultLogger.Info("MQTT Connecting")

	if token := client.Connect(); token.Wait() && token.Error() != nil {
		return nil, fmt.Errorf("error connecting to MQTT broker: %v", token.Error())
	}

	return &plugin{mqtt: client, channelPrefix: fmt.Sprintf("ds/%s/", s.UID)}, nil
}

type plugin struct {
	channelPrefix string
	mqtt          paho.Client
	// cache stores the last data frame transmitted to a channel.
	// This way allows other clients connecting to the
	// streaming channel to receive the last value instead of no data.
	cache sync.Map
}

var (
	_ backend.QueryDataHandler      = (*plugin)(nil)
	_ backend.CheckHealthHandler    = (*plugin)(nil)
	_ backend.StreamHandler         = (*plugin)(nil)
	_ instancemgmt.InstanceDisposer = (*plugin)(nil)
)

func (p *plugin) CheckHealth(_ context.Context, _ *backend.CheckHealthRequest) (*backend.CheckHealthResult, error) {
	if !p.mqtt.IsConnected() {
		return &backend.CheckHealthResult{
			Status:  backend.HealthStatusError,
			Message: "MQTT Disconnected",
		}, nil
	}
	return &backend.CheckHealthResult{
		Status:  backend.HealthStatusOk,
		Message: "MQTT Connected",
	}, nil
}

func (p *plugin) SubscribeStream(_ context.Context, req *backend.SubscribeStreamRequest) (*backend.SubscribeStreamResponse, error) {
	return &backend.SubscribeStreamResponse{
		Status: backend.SubscribeStreamStatusOK,
	}, nil
}

func (p *plugin) PublishStream(_ context.Context, _ *backend.PublishStreamRequest) (*backend.PublishStreamResponse, error) {
	return &backend.PublishStreamResponse{
		Status: backend.PublishStreamStatusPermissionDenied,
	}, nil
}

func (p *plugin) RunStream(ctx context.Context, req *backend.RunStreamRequest, sender *backend.StreamSender) error {
	log.DefaultLogger.Info(fmt.Sprintf("subscribing to topic %v", req.Path))
	if t := p.mqtt.Subscribe(req.Path, 0, func(_ paho.Client, msg paho.Message) {
		log.DefaultLogger.Info(fmt.Sprintf("received message %s", msg.Payload()))
		f := p.frame(req.Path,
			data.NewField("time", nil, []time.Time{time.Now()}),
			data.NewField("value", nil, []string{string(msg.Payload())}))
		p.cache.Store(req.Path, f)
		if err := sender.SendFrame(f, data.IncludeAll); err != nil {
			log.DefaultLogger.Error(fmt.Sprintf("unable to send message: %v", err))
		}
	}); t.Wait() && t.Error() != nil {
		return t.Error()
	}
	<-ctx.Done()
	backend.Logger.Info(fmt.Sprintf("stop streaming (context canceled) topic: %v", req.Path))
	t := p.mqtt.Unsubscribe(req.Path)
	t.Wait()
	p.cache.Delete(req.Path)
	return t.Error()
}

func (p *plugin) QueryData(_ context.Context, req *backend.QueryDataRequest) (*backend.QueryDataResponse, error) {
	response := backend.NewQueryDataResponse()
	for _, query := range req.Queries {
		var model struct {
			Topic string `json:"queryText"`
		}
		response.Responses[query.RefID] = backend.DataResponse{
			Error:  json.Unmarshal(query.JSON, &model),
			Frames: data.Frames{p.frame(model.Topic)},
		}
	}
	return response, nil
}

func (p *plugin) Dispose() {
	log.DefaultLogger.Info("MQTT Disconnecting")
	p.mqtt.Disconnect(250)
}

func (p *plugin) frame(topic string, fields ...*data.Field) *data.Frame {
	if len(fields) == 0 {
		if f, ok := p.cache.Load(topic); ok {
			return f.(*data.Frame)
		}
	}
	return data.NewFrame(topic, fields...).SetMeta(&data.FrameMeta{Channel: p.channelPrefix + topic})
}
