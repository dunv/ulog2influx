package ulog2influx

import (
	"fmt"
	"strings"
	"time"

	influx "github.com/influxdata/influxdb1-client/v2"
)

type Ulog2InfluxWriter struct {
	InfluxMetricName    string
	InfluxURL           string
	InfluxUser          string
	InfluxPassword      string
	InfluxDB            string
	AdditionalTags      map[string]string
	AdditionalFields    map[string]interface{}
	Client              influx.Client
	LogLineChannel      chan *influx.Point
	TimeZone            *time.Location
	FlushInterval       time.Duration
	SeparationCharacter string
	FieldMapping        map[int]string
}

func NewUlog2InfluxWriter(
	influxMetricName string,
	influxURL string,
	influxUser string,
	influxPassword string,
	influxDB string,
	flushInterval time.Duration,
	separationCharacter *string,
	fieldMapping map[int]string,
) (*Ulog2InfluxWriter, error) {
	c, err := influx.NewHTTPClient(influx.HTTPConfig{
		Addr:     influxURL,
		Username: influxUser,
		Password: influxPassword,
	})
	if err != nil {
		return nil, err
	}
	logLineChannel := make(chan *influx.Point)
	go asyncPushRoutine(c, influxDB, -1, logLineChannel, flushInterval)

	usedSeparationCharacter := " | "
	if separationCharacter != nil {
		usedSeparationCharacter = *separationCharacter
	}

	usedFieldMapping := map[int]string{
		0: "ts",
		1: "tag:log_level",
		2: "field:location",
		3: "field:message",
	}
	if fieldMapping != nil {
		usedFieldMapping = fieldMapping
	}

	writer := &Ulog2InfluxWriter{
		InfluxMetricName:    influxMetricName,
		InfluxURL:           influxURL,
		InfluxUser:          influxUser,
		InfluxPassword:      influxPassword,
		InfluxDB:            influxDB,
		Client:              c,
		LogLineChannel:      logLineChannel,
		TimeZone:            time.UTC,
		FlushInterval:       flushInterval,
		SeparationCharacter: usedSeparationCharacter,
		FieldMapping:        usedFieldMapping,
	}
	return writer, nil
}

func (w *Ulog2InfluxWriter) SetAdditionalTags(additionalTags map[string]string) {
	w.AdditionalTags = additionalTags
}

func (w *Ulog2InfluxWriter) SetAdditionalFields(additionalFields map[string]interface{}) {
	w.AdditionalFields = additionalFields
}

func (w *Ulog2InfluxWriter) SetTimezone(tz *time.Location) {
	w.TimeZone = tz
}

func (w *Ulog2InfluxWriter) Write(p []byte) (n int, err error) {
	parts := strings.Split(string(p), w.SeparationCharacter)

	if len(parts) < len(w.FieldMapping) {
		fmt.Println("could not send logline to influx, parsing err (line has to few parts)")
		return 0, fmt.Errorf("could not send logline to influx, parsing err (line has to few parts)")
	}

	tags := map[string]string{}
	for name, value := range w.AdditionalTags {
		tags[name] = value
	}
	fields := map[string]interface{}{}
	for name, value := range w.AdditionalFields {
		fields[name] = value
	}

	var parsedTime time.Time
	count := 0
	for index, classification := range w.FieldMapping {
		content := parts[index]
		// Figure out which field is the last one and merge all parts after that into one
		if count == len(w.FieldMapping) {
			content = strings.Join(parts[3:], w.SeparationCharacter)
		}
		count++
		// Clean up
		content = strings.TrimSpace(content)
		content = strings.ReplaceAll(content, `\`, `/`)
		content = strings.ReplaceAll(content, `"`, `\"`)
		content = strings.TrimSuffix(content, "\n")

		if classification == "ts" {
			parsedTime, err = time.ParseInLocation("2006-01-02 15:04:05.000", content, w.TimeZone)
			if err != nil {
				fmt.Println("could not parse time", err)
				return 0, fmt.Errorf("could not parse time (%s)", err)
			}
			continue
		}
		classificationParts := strings.Split(classification, ":")
		if len(classificationParts) != 2 {
			fmt.Println("classification needs to contain field:fieldname or tag:tagname")
			return 0, fmt.Errorf("classification needs to contain field:fieldname or tag:tagname")
		}

		if classificationParts[0] == "tag" {
			tags[classificationParts[1]] = content
			continue
		}
		if classificationParts[0] == "field" {
			fields[classificationParts[1]] = content
			continue
		}
	}

	point, err := influx.NewPoint(w.InfluxMetricName, tags, fields, parsedTime)
	if err != nil {
		fmt.Println("could not create point", err)
		return 0, fmt.Errorf("could not create point (%s)", err)
	}

	w.LogLineChannel <- point

	return len(p), nil
}
