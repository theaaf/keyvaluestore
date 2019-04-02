package dynamodbstore

import (
	"github.com/aws/aws-dax-go/dax"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

type DaxBackendClient struct {
	*dax.Dax
}

func (c *DaxBackendClient) TransactWriteItems(input *dynamodb.TransactWriteItemsInput) (*dynamodb.TransactWriteItemsOutput, TransactWriteErr) {
	output, err := c.Dax.TransactWriteItems(input)
	if err == nil {
		return output, nil
	}

	if cancelledErr, ok := err.(dax.DaxTransactionCanceledFailure); ok {
		ret := &transactWriteErr{
			awsErr:              cancelledErr,
			cancellationReasons: make([]awserr.Error, len(cancelledErr.CancellationReasonCodes())),
		}
		messages := cancelledErr.CancellationReasonMessages()
		for i, code := range cancelledErr.CancellationReasonCodes() {
			if code != "" && code != "None" && i < len(messages) {
				ret.cancellationReasons[i] = awserr.New(code, messages[i], nil)
			}
		}
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
