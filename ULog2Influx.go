package ulog2influx

import (
	"fmt"
	"strings"
	"time"

	influx "github.com/influxdata/influxdb1-client/v2"
)

type Ulog2InfluxWriter struct {
	InfluxMetricName string
	InfluxURL        string
	InfluxUser       string
	InfluxPassword   string
	InfluxDB         string
	AdditionalTags   map[string]string
	AdditionalFields map[string]interface{}
	Client           influx.Client
	LogLineChannel   chan *influx.Point
	TimeZone         *time.Location
}

func NewUlog2InfluxWriter(
	influxMetricName string,
	influxURL string,
	influxUser string,
	influxPassword string,
	influxDB string,
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
	go asyncPushRoutine(c, influxDB, -1, logLineChannel)
	writer := &Ulog2InfluxWriter{
		InfluxMetricName: influxMetricName,
		InfluxURL:        influxURL,
		InfluxUser:       influxUser,
		InfluxPassword:   influxPassword,
		InfluxDB:         influxDB,
		Client:           c,
		LogLineChannel:   logLineChannel,
		TimeZone:         time.UTC,
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
	parts := strings.Split(string(p), " | ")

	if len(parts) < 5 {
		fmt.Println("could not send logline to influx, parsing err (line has to few parts)")
		return 0, fmt.Errorf("could not send logline to influx, parsing err (line has to few parts)")
	}

	parsedTime, err := time.ParseInLocation("2006-01-02 15:04:05.000", strings.TrimSpace(parts[0]), w.TimeZone)
	if err != nil {
		fmt.Println("could not parse time", err)
		return 0, fmt.Errorf("could not parse time (%s)", err)
	}

	parsedLogLevel := strings.TrimSpace(parts[1])
	parsedThread := strings.TrimSpace(parts[2])
	parsedLocation := strings.TrimSpace(parts[3])

	// Take the rest of the line and escape characters for influx
	parsedMessage := strings.Join(parts[4:], " | ")
	parsedMessage = strings.ReplaceAll(parsedMessage, `\`, `/`)
	parsedMessage = strings.ReplaceAll(parsedMessage, `"`, `\"`)
	parsedMessage = strings.TrimSuffix(parsedMessage, "\n")

	// fmt.Println("time", parsedTime)
	// fmt.Println("level", parsedLogLevel)
	// fmt.Println("thread", parsedThread)
	// fmt.Println("location", parsedLocation)
	// fmt.Println("message", parsedMessage)

	tags := map[string]string{
		"log_level": parsedLogLevel,
	}

	for name, value := range w.AdditionalTags {
		tags[name] = value
	}

	fields := map[string]interface{}{
		"thread":   parsedThread,
		"location": parsedLocation,
		"message":  parsedMessage,
	}

	for name, value := range w.AdditionalFields {
		fields[name] = value
	}

	point, err := influx.NewPoint(w.InfluxMetricName, tags, fields, parsedTime)
	if err != nil {
		fmt.Println("could not create point", err)
		return 0, fmt.Errorf("could not create point (%s)", err)
	}

	w.LogLineChannel <- point

	return len(p), nil
}

// influxLine = fmt.Sprintf("%s,%s,log_level=%s,service_name=%s count=%d,thread=\"%s\",location=\"%s\",message=\"%s\"%s%s %d",
