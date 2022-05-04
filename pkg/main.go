package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
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

	opts := paho.NewClientOptions()

	opts.AddBroker(fmt.Sprintf("tcp://%s:%d", settings.Host, settings.Port))
	opts.SetClientID(fmt.Sprintf("grafana_%d", rand.Int()))

	if settings.Username != "" {
		opts.SetUsername(settings.Username)
	}

	if settings.Password != "" {
		opts.SetPassword(settings.Password)
	}

	opts.SetPingTimeout(60 * time.Second)
	opts.SetKeepAlive(60 * time.Second)
	opts.SetAutoReconnect(true)
	opts.SetMaxReconnectInterval(10 * time.Second)
	opts.SetConnectionLostHandler(func(_ paho.Client, err error) {
		log.DefaultLogger.Error(fmt.Sprintf("MQTT Connection Lost: %s", err.Error()))
	})
	opts.SetReconnectingHandler(func(_ paho.Client, _ *paho.ClientOptions) {
		log.DefaultLogger.Debug("MQTT Reconnecting")
	})

	log.DefaultLogger.Info("MQTT Connecting")

	client := paho.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		return nil, fmt.Errorf("error connecting to MQTT broker: %s", token.Error())
	}

	return &plugin{
		mqtt:          client,
		channelPrefix: fmt.Sprintf("ds/%s/", s.UID),
	}, nil
}

type plugin struct {
	channelPrefix string
	mqtt          paho.Client
}

// Make sure MQTTDatasource implements required interfaces.
// This is important to do since otherwise we will only get a
// not implemented error response from plugin in runtime.
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
	if t := p.mqtt.Subscribe(req.Path, 0, func(_ paho.Client, msg paho.Message) {
		log.DefaultLogger.Info(fmt.Sprintf("Received message: %s", msg.Payload()))
		/*message := struct {
			Time  time.Time       `json:"time"`
			Value json.RawMessage `json:"value"`
		}{time.Now(), msg.Payload()}
		raw, err := json.Marshal(message)
		log.DefaultLogger.Info(fmt.Sprintf("Sending: %s", raw))
		if err != nil {
			log.DefaultLogger.Error(fmt.Sprintf("unable to send message: %v", err))
		} else */if err := sender.SendJSON(msg.Payload()); err != nil {
			log.DefaultLogger.Error(fmt.Sprintf("unable to send message: %v", err))
		}
	}); t.Wait() && t.Error() != nil {
		return t.Error()
	}
	<-ctx.Done()
	backend.Logger.Info("stop streaming (context canceled)")
	t := p.mqtt.Unsubscribe(req.Path)
	t.Wait()
	return t.Error()
}

func (p *plugin) QueryData(_ context.Context, req *backend.QueryDataRequest) (*backend.QueryDataResponse, error) {
	response := backend.NewQueryDataResponse()

	for _, query := range req.Queries {
		var model struct {
			Topic string `json:"queryText"`
		}
		res := backend.DataResponse{}
		res.Error = json.Unmarshal(query.JSON, &model)
		res.Frames = append(res.Frames, data.NewFrame(model.Topic).SetMeta(&data.FrameMeta{
			Channel: p.channelPrefix + model.Topic,
		}))
		response.Responses[query.RefID] = res
		log.DefaultLogger.Error(fmt.Sprintf("Model: %v", p.channelPrefix+model.Topic))
	}
	return response, nil
}

func (p *plugin) Dispose() {
	log.DefaultLogger.Info("MQTT Disconnecting")
	p.mqtt.Disconnect(250)
}
