package plugin

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/data"
	"github.com/grafana/mqtt-datasource/pkg/mqtt"
)

func ToFrame(fields map[string]*data.Field, topic string, messages []mqtt.Message) *data.Frame {
	count := len(messages)
	if count > 0 {
		first := messages[0].Value
		if strings.HasPrefix(first, "{") {
			return jsonMessagesToFrame(fields, topic, messages)
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

func jsonMessagesToFrame(fields map[string]*data.Field, topic string, messages []mqtt.Message) *data.Frame {
	count := len(messages)
	if count == 0 {
		return nil
	}

	/*err := json.Unmarshal([]byte(messages[0].Value), &body)
	if err != nil {
		frame := data.NewFrame(topic)
		frame.AppendNotices(data.Notice{Severity: data.NoticeSeverityError,
			Text: fmt.Sprintf("error unmarshalling json message: %s", err.Error()),
		})
		return frame
	}
	body = checkmapstructure(body)*/
	timeField := data.NewFieldFromFieldType(data.FieldTypeTime, count)
	timeField.Name = "Time"
	frame := data.NewFrame(topic, timeField)
	// Create a field for each key and set the values of all rows
	for row, m := range messages {
		timeField.SetConcrete(row, m.Timestamp)
		var body map[string]interface{}
		err := json.Unmarshal([]byte(m.Value), &body)
		//keys := make([]string, 0, len(checkmapstructure(body)))

		if err != nil {
			continue // bad row?
		}
		for key, val := range checkmapstructure(body) {
			field, exists := fields[key]
			if !exists {
				var t data.FieldType
				switch val.(type) {
				case float64:
					t = data.FieldTypeNullableFloat64
				case string:
					t = data.FieldTypeNullableString
				default:
					t = data.FieldTypeUnknown
				}
				field = data.NewFieldFromFieldType(t, count)
				field.Name = key
				fields[key] = field
				//keys = append(keys, key)
			}

			field.SetConcrete(row, val)

		}

		//sort.Strings(keys)
		backend.Logger.Info(fmt.Sprintf("number of iterations: %v", row))

	}
	for _, val := range fields {
		frame.Fields = append(frame.Fields, val)
	}
	sort.Slice(frame.Fields, func(i, j int) bool {
		return frame.Fields[i].Name < frame.Fields[j].Name
	})
	return frame

}
