package e2e_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/typusomega/goethe/pkg/client"
	"github.com/typusomega/goethe/pkg/spec"
	"github.com/wcharczuk/go-chart"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestBasic(t *testing.T) {
	serverAddress, err := locateGoetheServer()
	if err != nil {
		t.Log(err.Error())
		t.Skip()
	}

	t.Log("starting basic e2e test")
	ctx, cncl := context.WithTimeout(context.Background(), 10*time.Second)
	defer cncl()

	lastEvent := 10000
	publishTimestamps := make([]int64, lastEvent)
	consumptionTimestamps := make([]int64, lastEvent)

	publisher, err := client.New(ctx, serverAddress)
	assert.Nil(t, err)

	consumer, err := client.New(ctx, serverAddress)
	assert.Nil(t, err)

	topic := &spec.Topic{
		Id: uuid.New().String(),
	}

	go func() {
		t.Log("starting publishing")
		for index := 0; index < lastEvent; index++ {
			publishTimestamps[index] = time.Now().UnixNano()
			_, err := publisher.Produce(ctx, topic, []byte(strconv.Itoa(index)))
			assert.Nil(t, err)
		}
	}()

	cursors := make(chan *spec.Cursor)
	success := false

	t.Log("starting consumption")
	go func() {
		for {
			err = consumer.ConsumeBlocking(ctx, &spec.Cursor{Topic: topic, Consumer: "default", CurrentEvent: &spec.Event{Topic: topic}}, cursors)
			if err != nil {
				if err == io.EOF {
					continue
				}
				status, _ := status.FromError(err)
				switch status.Code() {
				case codes.ResourceExhausted:
					continue
				case codes.Canceled:
					return
				default:
					panic(err)
				}
			}
		}

	}()

	lastValue := -1
	for !success {
		select {
		case <-ctx.Done():
			t.Logf("CLOSING AFTER %v; GOT %d", consumptionTimestamps[lastValue], lastValue)
			assert.True(t, success)
			return

		case cursor := <-cursors:
			value, err := strconv.Atoi(string(cursor.GetCurrentEvent().GetPayload()))
			assert.Nil(t, err)
			assert.True(t, lastValue+1 == value || lastValue == value)
			lastValue = value
			consumptionTimestamps[lastValue] = time.Now().UnixNano()

			if value == lastEvent-1 {
				success = true
			}
		}
	}

	outFile, err := os.Create(fmt.Sprintf("out_e2e_basic_%s.png", time.Now().Format("RFC3339")))
	if err != nil {
		panic(err)
	}
	defer outFile.Close()

	xVals := make([]float64, lastEvent)
	durations := make([]float64, lastEvent)
	for index := 0; index < lastEvent; index++ {
		xVals[index] = float64(index)
		durations[index] = float64(consumptionTimestamps[index] - publishTimestamps[index])
	}

	graph := chart.Chart{
		XAxis: chart.XAxis{
			Name: "event nr",
			ValueFormatter: func(v interface{}) string {
				if vf, isFloat := v.(float64); isFloat {
					return fmt.Sprintf("%.0f", vf)
				}
				return ""
			},
		},
		YAxis: chart.YAxis{
			Name:  "duration",
			Style: chart.StyleShow(),
			ValueFormatter: func(v interface{}) string {
				if vf, isFloat := v.(float64); isFloat {
					return fmt.Sprintf("%.2f", vf/1000000)
				}
				return ""
			},
		},

		Series: []chart.Series{
			chart.ContinuousSeries{
				Style: chart.Style{
					StrokeColor: chart.GetDefaultColor(0),
					Show:        true,
				},
				YValues: durations,
				XValues: xVals,
			},
		},
	}

	buffer := bytes.NewBuffer([]byte{})

	err = graph.Render(chart.PNG, buffer)
	if err != nil {
		panic(err)
	}

	if _, err := outFile.Write(buffer.Bytes()); err != nil {
		panic(err)
	}
}

func locateGoetheServer() (string, error) {
	env := os.Getenv("GOETHE_SERVER")
	if env == "" {
		return "", errors.New("GoETHE_SERVER envvar not set")
	}
	return env, nil
}
