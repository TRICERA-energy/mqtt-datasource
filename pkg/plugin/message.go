package plugin

import (
	"encoding/json"
	"sort"
	"strconv"
	"strings"

	"github.com/grafana/grafana-plugin-sdk-go/data"
	"github.com/grafana/mqtt-datasource/pkg/mqtt"
)

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

func jsonMessagesToFrame(topic string, messages []mqtt.Message) *data.Frame {
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
	fields := make(map[string]*data.Field)
	// Create a field for each key and set the values of all rows
	for row, m := range messages {
		timeField.SetConcrete(row, m.Timestamp)
		var body map[string]interface{}
		err := json.Unmarshal([]byte(m.Value), &body)
		if err != nil {
			continue // bad row?
		}
		for key, val := range checkmapstructure(body) {
			f, exists := fields[key]
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
				f = data.NewFieldFromFieldType(t, count)
				fields[key] = f
			}
			f.SetConcrete(row, val)
		}
	}
	frame := data.NewFrame(topic, timeField)
	for _, val := range fields {
		frame.Fields = append(frame.Fields, val)
	}
	sort.Slice(frame.Fields, func(i, j int) bool {
		return frame.Fields[i].Name < frame.Fields[j].Name
	})
	return frame
	/*
		// Add rows 1...n
		for row, m := range messages {
			err := json.Unmarshal([]byte(m.Value), &body) // this  is a value
			if err != nil {
				continue // bad row?
			}
			body = checkmapstructure(body)
			timeField.SetConcrete(row, m.Timestamp)
			for key, val := range body {
				field, ok := fields[key]
				if ok {
					field.SetConcrete(row, val)
				}
			}
		}
	*/

}
