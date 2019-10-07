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
	produceDurations := make([]float64, lastEvent)
	consumptionDurations := make([]float64, lastEvent)

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
			start := time.Now()
			_, err := publisher.Produce(ctx, topic, []byte(strconv.Itoa(index)))
			produceDurations[index] = float64(time.Since(start).Microseconds())
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
		start := time.Now()
		select {
		case <-ctx.Done():
			t.Logf("CLOSING AFTER %v; GOT %d", consumptionDurations[lastValue], lastValue)
			assert.True(t, success)
			return

		case cursor := <-cursors:
			value, err := strconv.Atoi(string(cursor.GetCurrentEvent().GetPayload()))
			consumptionDurations[value] = float64(time.Since(start).Microseconds())
			assert.Nil(t, err)
			assert.True(t, lastValue+1 == value || lastValue == value)
			lastValue = value

			if value == lastEvent-1 {
				success = true
			}
		}
	}

	outFile, err := os.Create(fmt.Sprintf("out_e2e_basic_%s.svg", time.Now().Format(time.RFC3339)))
	if err != nil {
		panic(err)
	}
	defer outFile.Close()

	xVals := make([]float64, lastEvent)
	for index := 0; index < lastEvent; index++ {
		xVals[index] = float64(index)
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
			Name:      "duration in µ-secs",
			NameStyle: chart.StyleShow(),
			Style:     chart.StyleShow(),
			ValueFormatter: func(v interface{}) string {
				if vf, isFloat := v.(float64); isFloat {
					return fmt.Sprintf("%.0f", vf)
				}
				return ""
			},
		},

		Series: []chart.Series{
			chart.ContinuousSeries{
				Style: chart.Style{
					StrokeColor: chart.ColorRed,
					Show:        true,
				},
				Name:    "produce",
				YValues: produceDurations,
				XValues: xVals,
			},
			chart.ContinuousSeries{
				Style: chart.Style{
					StrokeColor: chart.ColorGreen,
					Show:        true,
				},
				Name:    "consume",
				YValues: consumptionDurations,
				XValues: xVals,
			},
		},
	}

	buffer := bytes.NewBuffer([]byte{})

	err = graph.Render(chart.SVG, buffer)
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
