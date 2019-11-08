package ulog2influx

import (
	"fmt"

	"github.com/dunv/concurrentList"
	"github.com/dunv/ulog"
	influx "github.com/influxdata/influxdb1-client/v2"
)

func asyncPushRoutine(
	client influx.Client,
	influxDB string,
	retry int,
	logLineChannel chan *influx.Point,
) chan bool {
	list := concurrentList.NewConcurrentList()
	returnChannel := make(chan bool)
	go func() {
		for logLine := range logLineChannel {
			list.Append(logLine)
		}
	}()

	go func() {
		for {
			logLine, err := list.GetNext()
			if err != nil {
				ulog.Warnf("Could not getNext from logLineChannelBuffer")
				continue
			}

			castedPoint := logLine.(*influx.Point)
			batchPoints, _ := influx.NewBatchPoints(influx.BatchPointsConfig{Database: influxDB})
			batchPoints.AddPoint(castedPoint)
			err = client.Write(batchPoints)
			if err != nil {
				fmt.Println("could not write point to influx", err)
			}
		}
	}()

	client.Close()
	return returnChannel
}
