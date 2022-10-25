package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sns/types"
	"github.com/stoewer/go-strcase"
)

const TimeFormat = "2006-01-02T15:04:05-0700"

type Handler struct {
	topic  string
	client *sns.Client
}

type Event struct {
	Source      string                  `json:"source"`
	ID          string                  `json:"id"`
	Type        string                  `json:"type"`
	Time        time.Time               `json:"time"`
	Subject     string                  `json:"subject"`
	User        string                  `json:"cloudkatauser"`
	ContentType string                  `json:"datacontenttype"`
	Data        *map[string]interface{} `json:"data"`
}

func (h *Handler) Handle(ctx context.Context, request events.DynamoDBEvent) error {

	events := make([]map[string]interface{}, 0)
	for _, record := range request.Records {
		if !isValid(record) {
			continue
		}
		event := make(map[string]interface{})
		event["source"] = getSource(record)
		event["id"] = getId(record)
		event["type"] = getType(record)
		event["time"] = getTime(record)
		event["subject"] = getSubject(record)
		event["user"] = getUser(record)
		event["datacontenttype"] = "application/json"
		event["data"] = getData(record)

		events = append(events, event)
	}

	if len(events) > 0 {
		entries := make([]types.PublishBatchRequestEntry, 0)
		/*for _, item := range events {
			entries = append(entries, types.PublishBatchRequestEntry{})
		}*/
		h.client.PublishBatch(ctx, &sns.PublishBatchInput{
			TopicArn:                   &h.topic,
			PublishBatchRequestEntries: entries,
		})
	}

	return nil
}

func isValid(record events.DynamoDBEventRecord) bool {
	typename := record.Change.NewImage["__typename"].String()
	id := record.Change.NewImage["id"].String()
	return typename != "" && id != ""
}

func getSource(record events.DynamoDBEventRecord) string {

	typename := record.Change.NewImage["__typename"].String()
	id := record.Change.NewImage["id"].String()
	return fmt.Sprint("/%s/%s", strcase.KebabCase(typename), id)
}

func getId(record events.DynamoDBEventRecord) string {
	return record.EventID
}

func getType(record events.DynamoDBEventRecord) string {
	typename := record.Change.NewImage["__typename"].String()
	if record.Change.OldImage == nil && record.Change.NewImage != nil {
		return fmt.Sprintf("%sCreated", typename)
	}
	if record.Change.OldImage != nil && record.Change.NewImage != nil {
		return fmt.Sprintf("%sUpdated", typename)
	}
	return fmt.Sprintf("%sDeleted", typename)
}

func getTime(record events.DynamoDBEventRecord) time.Time {

	recordTime := record.Change.NewImage["__time"].String()
	if record.Change.NewImage != nil && recordTime != "" {
		eventTime, err := time.Parse(TimeFormat, recordTime)
		if err == nil {
			return eventTime
		}
	}
	return record.Change.ApproximateCreationDateTime.Time
}

func getSubject(record events.DynamoDBEventRecord) *string {
	subject := record.Change.NewImage["__user"].String()
	if subject == "" {
		return nil
	}
	return &subject
}

func getUser(record events.DynamoDBEventRecord) string {
	user := record.Change.NewImage["__user"].String()
	if user == "" {
		return "unknown"
	}
	return user
}

func getData(record events.DynamoDBEventRecord) map[string]interface{} {
	return nil
}

func main() {
	region := os.Getenv("AWS_REGION")
	topic := os.Getenv("CVX_EVENT_BUS")

	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion(region),
	)
	if err != nil {
		log.Fatalf("unable to load SDK config, %v", err)
	}

	client := sns.NewFromConfig(cfg)

	handler := &Handler{
		topic:  topic,
		client: client,
	}

	lambda.Start(handler.Handle)
}
