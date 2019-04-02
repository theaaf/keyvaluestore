package dynamodbstore

import (
	"io/ioutil"
	"strings"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/private/protocol/jsonrpc"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	jsoniter "github.com/json-iterator/go"
)

type AWSBackendClient struct {
	dynamodbiface.DynamoDBAPI
}

type jsonErrorResponse struct {
	Type                string `json:"__type"`
	Message             string
	CancellationReasons []*struct {
		Code    string
		Message string
	}
}

type awsErr awserr.Error

type transactWriteErr struct {
	awsErr
	statusCode          int
	requestId           string
	cancellationReasons []awserr.Error
}

func (err *transactWriteErr) StatusCode() int {
	return err.statusCode
}

func (err *transactWriteErr) RequestID() string {
	return err.requestId
}

func (err *transactWriteErr) CancellationReasons() []awserr.Error {
	return err.cancellationReasons
}

func (c *AWSBackendClient) TransactWriteItems(input *dynamodb.TransactWriteItemsInput) (*dynamodb.TransactWriteItemsOutput, TransactWriteErr) {
	req, output := c.DynamoDBAPI.TransactWriteItemsRequest(input)

	if !req.Handlers.UnmarshalError.SwapNamed(request.NamedHandler{
		Name: jsonrpc.UnmarshalErrorHandler.Name,
		Fn: func(req *request.Request) {
			defer req.HTTPResponse.Body.Close()
			bodyBytes, err := ioutil.ReadAll(req.HTTPResponse.Body)
			if err != nil {
				req.Error = awserr.New("SerializationError", "failed reading JSON RPC error response", err)
				return
			}
			if len(bodyBytes) == 0 {
				req.Error = awserr.NewRequestFailure(
					awserr.New("SerializationError", req.HTTPResponse.Status, nil),
					req.HTTPResponse.StatusCode,
					"",
				)
				return
			}
			var jsonErr jsonErrorResponse
			if err := jsoniter.Unmarshal(bodyBytes, &jsonErr); err != nil {
				req.Error = awserr.New("SerializationError", "failed decoding JSON RPC error response", err)
				return
			}

			codes := strings.SplitN(jsonErr.Type, "#", 2)
			transactWriteErr := &transactWriteErr{
				awsErr:              awserr.New(codes[len(codes)-1], jsonErr.Message, nil),
				cancellationReasons: make([]awserr.Error, len(jsonErr.CancellationReasons)),
				statusCode:          req.HTTPResponse.StatusCode,
				requestId:           req.RequestID,
			}
			for i, reason := range jsonErr.CancellationReasons {
				if reason != nil && reason.Code != "None" {
					transactWriteErr.cancellationReasons[i] = awserr.New(reason.Code, reason.Message, nil)
				}
			}
			req.Error = transactWriteErr
		},
	}) {
		return nil, &transactWriteErr{
			awsErr: awserr.New("HandlerError", "failed to replace default error handler", nil),
		}
	}

	err := req.Send()

	if err == nil {
		return output, nil
	}

	if ret, ok := err.(TransactWriteErr); ok {
		return nil, ret
	} else if awsErr, ok := err.(awserr.Error); ok {
		return nil, &transactWriteErr{
			awsErr: awsErr,
		}
	}

	return nil, &transactWriteErr{
		awsErr: awserr.New("UnknownError", err.Error(), err),
	}
}
