package dynamodbstore

import (
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

type TransactWriteErr interface {
	awserr.RequestFailure
	CancellationReasons() []awserr.Error
}

type BackendClient interface {
	BatchGetItem(input *dynamodb.BatchGetItemInput) (*dynamodb.BatchGetItemOutput, error)
	BatchWriteItem(input *dynamodb.BatchWriteItemInput) (*dynamodb.BatchWriteItemOutput, error)
	DeleteItem(*dynamodb.DeleteItemInput) (*dynamodb.DeleteItemOutput, error)
	GetItem(*dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error)
	PutItem(*dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error)
	Query(*dynamodb.QueryInput) (*dynamodb.QueryOutput, error)
	UpdateItem(*dynamodb.UpdateItemInput) (*dynamodb.UpdateItemOutput, error)

	TransactWriteItems(*dynamodb.TransactWriteItemsInput) (*dynamodb.TransactWriteItemsOutput, TransactWriteErr)
}
