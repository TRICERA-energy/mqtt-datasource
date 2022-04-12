package plugin

import (
	"encoding/json"
	"sort"
	"strconv"
	"strings"

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

//append two maps
func appendmaps(firstmap map[string]interface{}, secondmap map[string]interface{}, secondmapkey string) map[string]interface{} {
	for k, v := range firstmap {
		secondmap[secondmapkey+k] = v
	}
	return secondmap
}

//change map structure from nested to normal key value map
func checkmapstructure(x map[string]interface{}) map[string]interface{} {
	newmap := make(map[string]interface{})

	for k, v := range x {
		switch v := v.(type) {
		case map[string]interface{}: //Object
			var b strings.Builder
			b.WriteString(k + ".")
			appendmaps(checkmapstructure(v), newmap, b.String())
		case []interface{}: //Array
			for newkey := 0; newkey < len(v); newkey++ {
				var b strings.Builder
				b.WriteString(k + "[" + strconv.Itoa(newkey) + "]")
				newmap[b.String()] = v[newkey]
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

	timeField := data.NewFieldFromFieldType(data.FieldTypeTime, count)
	timeField.Name = "Time"

	// Create a field for each key and for each row set the values
	for row, m := range messages {
		timeField.SetConcrete(row, m.Timestamp)
		var body map[string]interface{}
		err := json.Unmarshal([]byte(m.Value), &body)

		if err != nil {
			return nil // bad row?
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
				case bool:
					t = data.FieldTypeNullableBool
				default: //null
					t = data.FieldTypeUnknown
				}
				field = data.NewFieldFromFieldType(t, count)
				field.Name = key
				field.SetConcrete(row, val)
				fields[key] = field
			} else {
				switch val.(type) {
				case float64:
					if exists && field.Type() == data.FieldTypeNullableFloat64 {
						field.SetConcrete(row, val)
					} else {
						field = data.NewFieldFromFieldType(data.FieldTypeNullableFloat64, count)
						field.Name = key
						field.SetConcrete(row, val)
						fields[key] = field
					}
				case string:
					if field.Type() == data.FieldTypeNullableString {
						field.SetConcrete(row, val)
					} else {
						field = data.NewFieldFromFieldType(data.FieldTypeNullableString, count)
						field.Name = key
						field.SetConcrete(row, val)
						fields[key] = field
					}
				case bool:
					if field.Type() == data.FieldTypeNullableBool {
						field.SetConcrete(row, val)
					} else {
						field = data.NewFieldFromFieldType(data.FieldTypeNullableBool, count)
						field.Name = key
						field.SetConcrete(row, val)
						fields[key] = field
					}

				default: //nil fall
					if field.Type() == data.FieldTypeUnknown {
						field.SetConcrete(row, val)
					} else {
						field = data.NewFieldFromFieldType(data.FieldTypeUnknown, count)
						field.Name = key
						field.SetConcrete(row, val)
						fields[key] = field

					}
				}
			}

		}

	}
	frame := data.NewFrame(topic, timeField)

	for _, f := range fields {
		frame.Fields = append(frame.Fields, f)
	}

	sort.Slice(frame.Fields, func(i, j int) bool {
		return frame.Fields[i].Name < frame.Fields[j].Name
	})

	return frame
}
