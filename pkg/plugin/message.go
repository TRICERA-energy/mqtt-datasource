package plugin

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/grafana/grafana-plugin-sdk-go/data"
	"github.com/grafana/mqtt-datasource/pkg/mqtt"
)

//append's two maps
func appendmaps(x map[string]interface{}, y map[string]interface{}, key string) map[string]interface{} {
	for k, v := range x {
		y[key+k] = v
	}
	return y
}

//change map structure from nested to normal key value map
func checkmapstructure(x map[string]interface{}) map[string]interface{} {
	newmap := make(map[string]interface{})

	for k, v := range x {
		switch v := v.(type) {
		case map[string]interface{}: //Object
			newkey := ""
			newkey = newkey + k + "."
			appendmaps(checkmapstructure(v), newmap, newkey)
		case []interface{}: //Array
			for newkey := 0; newkey < len(v); {
				strnewkey := strconv.Itoa(newkey)
				strnewkey = k + "[" + strnewkey + "]"
				newmap[strnewkey] = v[newkey]
				newkey++
			}

		default: // Number (float64), String (string), Boolean (bool), Null (nil)
			newmap[k] = v
		}

	}
	return newmap
}

func ToFrame(topic string, messages []mqtt.Message) *data.Frame {
	count := len(messages)
	if count > 0 {
		first := messages[0].Value
		if strings.HasPrefix(first, "{") {
			return jsonMessagesToFrame(topic, messages)
		}
	}

	// Fall through to expecting values
	timeField := data.NewFieldFromFieldType(data.FieldTypeTime, count)
	timeField.Name = "Time"
	valueField := data.NewFieldFromFieldType(data.FieldTypeFloat64, count)
	valueField.Name = "Value"

	for idx, m := range messages {
		if value, err := strconv.ParseFloat(m.Value, 64); err == nil {
			timeField.Set(idx, m.Timestamp)
			valueField.Set(idx, value)
		}
	}

	return data.NewFrame(topic, timeField, valueField)
}

func jsonMessagesToFrame(topic string, messages []mqtt.Message) *data.Frame {
	count := len(messages)
	if count == 0 {
		return nil
	}

	var body map[string]interface{}
	err := json.Unmarshal([]byte(messages[0].Value), &body)
	if err != nil {
		frame := data.NewFrame(topic)
		frame.AppendNotices(data.Notice{Severity: data.NoticeSeverityError,
			Text: fmt.Sprintf("error unmarshalling json message: %s", err.Error()),
		})
		return frame
	}
	body = checkmapstructure(body)
	timeField := data.NewFieldFromFieldType(data.FieldTypeTime, count)
	timeField.Name = "Time"
	timeField.SetConcrete(0, messages[0].Timestamp)

	// Create a field for each key and set the first value
	keys := make([]string, 0, len(body))
	fields := make(map[string]*data.Field, len(body))
	for key, val := range body {
		switch val.(type) {
		case float64:
			field := data.NewFieldFromFieldType(data.FieldTypeNullableFloat64, count)
			field.Name = key
			field.SetConcrete(0, val)
			fields[key] = field
		case string:
			field := data.NewFieldFromFieldType(data.FieldTypeNullableString, count)
			field.Name = key
			field.SetConcrete(0, val)
			fields[key] = field

		default:
			field := data.NewFieldFromFieldType(data.FieldTypeUnknown, count)
			field.Name = key
			field.SetConcrete(0, val)
			fields[key] = field
		}

		keys = append(keys, key)
	}
	sort.Strings(keys) // keys stable field order.

	// Add rows 1...n
	for row, m := range messages {
		if row == 0 {
			continue
		}

		err := json.Unmarshal([]byte(m.Value), &body)
		body = checkmapstructure(body)
		if err != nil {
			continue // bad row?
		}

		timeField.SetConcrete(row, m.Timestamp)
		for key, val := range body {
			field := fields[key]
			field.SetConcrete(row, val)
		}
	}

	frame := data.NewFrame(topic, timeField)
	for _, key := range keys {
		frame.Fields = append(frame.Fields, fields[key])
	}
	return frame
}
