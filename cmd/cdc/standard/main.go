package main

import (
	"context"

	"github.com/aws/aws-lambda-go/lambda"
)

type Request struct {
}

type Handler struct {
}

func (h *Handler) Handle(ctx context.Context, request Request) error {
	return nil
}

func main() {
	handler := &Handler{}
	lambda.Start(handler.Handle)
}
