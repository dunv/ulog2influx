package ulog2influx

import (
	"fmt"
	"time"

	"github.com/dunv/concurrentList"
	influx "github.com/influxdata/influxdb1-client/v2"
)

func asyncPushRoutine(
	client influx.Client,
	influxDB string,
	retry int,
	logLineChannel chan *influx.Point,
	flushInterval time.Duration,
) chan bool {
	list := concurrentList.NewConcurrentList()
	returnChannel := make(chan bool)
	go func() {
		for logLine := range logLineChannel {
			list.Append(logLine)
		}
	}()

	go func() {
		var previousCastedPoint *influx.Point
		for {
			logLines := list.DeleteWithFilter(func(item interface{}) bool { return true })
			if len(logLines) == 0 {
				// fmt.Println("sleeping")
				time.Sleep(flushInterval)
				continue
			}

			castedPoints := []*influx.Point{}
			for _, point := range logLines {
				castedPoint := point.(*influx.Point)
				// make sure, that no two points are in the exact same nanosecond
				if previousCastedPoint != nil {
					timeDiff := previousCastedPoint.Time().Sub(castedPoint.Time())
					if timeDiff.Milliseconds() < 1 {
						// fmt.Println("detected same millisecond -> increasing by on nanosecond")
						castedPointFields, err := castedPoint.Fields()
						if err != nil {
							continue
						}
						castedPoint, err = influx.NewPoint(
							castedPoint.Name(),
							castedPoint.Tags(),
							castedPointFields,
							previousCastedPoint.Time().Add(time.Nanosecond),
						)
						if err != nil {
							continue
						}
					}

				}

				castedPoints = append(castedPoints, castedPoint)
				previousCastedPoint = castedPoint
			}

			batchPoints, _ := influx.NewBatchPoints(influx.BatchPointsConfig{Database: influxDB})
			batchPoints.AddPoints(castedPoints)
			err := client.Write(batchPoints)
			if err != nil {
				fmt.Println("could not write point to influx", err)
			}
			// fmt.Println("sending", batchPoints)
			time.Sleep(time.Second * 5)
		}
	}()

	client.Close()
	return returnChannel
}
