package mqtt

import (
	"path"
	"sync"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/data"
)

type Message struct {
	Timestamp time.Time
	Value     []byte
}

type GJSONPath struct {
	Path  string
	Alias string
}

type Topic struct {
	Path       string      `json:"topic"`
	GJSONPaths []GJSONPath `json:"gjsonpaths"`
	Interval   time.Duration
	Messages   []Message
	framer     *framer
}

func (t *Topic) Key() string {
	return path.Join(t.Interval.String(), t.Path)
}

func (t *Topic) ToDataFrame() (*data.Frame, error) {
	if t.framer == nil {
		t.framer = newFramer()
	}

	return t.framer.toFrame(t.Messages, t.GJSONPaths)
}

type TopicMap struct {
	sync.Map
}

func (tm *TopicMap) Load(path string) (*Topic, bool) {
	t, ok := tm.Map.Load(path)
	if !ok {
		return nil, false
	}

	topic, ok := t.(*Topic)
	return topic, ok
}

func (tm *TopicMap) AddGJSONPaths(path string, paths []GJSONPath) {
	t, ok := tm.Map.Load(path)
	if !ok {
		return
	}

	topic, ok := t.(*Topic)
	if !ok {
		return
	}

	topic.GJSONPaths = paths
	tm.Store(topic)
}

func (tm *TopicMap) AddMessage(path string, message Message) {
	tm.Map.Range(func(key, t any) bool {
		topic, ok := t.(*Topic)
		if !ok {
			return false
		}
		if topic.Path == path {
			topic.Messages = append(topic.Messages, message)
			tm.Store(topic)
		}
		return true
	})
}

func (tm *TopicMap) Store(t *Topic) {
	tm.Map.Store(t.Key(), t)
}

func (tm *TopicMap) Delete(path string) {
	tm.Map.Delete(path)
}
