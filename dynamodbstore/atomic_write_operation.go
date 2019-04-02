package dynamodbstore

import (
	"crypto/rand"
	"encoding/base64"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/pkg/errors"

	"github.aaf.cloud/platform/keyvaluestore"
)

type AtomicWriteOperation struct {
	Backend *Backend

	items   []*dynamodb.TransactWriteItem
	results []*atomicWriteResult
}

type atomicWriteResult struct {
	err awserr.Error
}

func (r *atomicWriteResult) ConditionalFailed() bool {
	return r.err != nil && r.err.Code() == "ConditionalCheckFailed"
}

func (op *AtomicWriteOperation) write(item dynamodb.TransactWriteItem) *atomicWriteResult {
	op.items = append(op.items, &item)
	ret := &atomicWriteResult{}
	op.results = append(op.results, ret)
	return ret
}

func (op *AtomicWriteOperation) SetNX(key string, value interface{}) keyvaluestore.AtomicWriteResult {
	return op.write(dynamodb.TransactWriteItem{
		Put: &dynamodb.Put{
			ConditionExpression: aws.String("attribute_not_exists(v)"),
			Item: newItem(key, "_", map[string]*dynamodb.AttributeValue{
				"v": attributeValue(value),
			}),
			TableName: &op.Backend.TableName,
		},
	})
}

func (op *AtomicWriteOperation) CAS(key string, oldValue, newValue string) keyvaluestore.AtomicWriteResult {
	return op.write(dynamodb.TransactWriteItem{
		Put: &dynamodb.Put{
			ConditionExpression: aws.String("v = :v"),
			ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
				":v": attributeValue(oldValue),
			},
			Item: newItem(key, "_", map[string]*dynamodb.AttributeValue{
				"v": attributeValue(newValue),
			}),
			TableName: &op.Backend.TableName,
		},
	})
}

func (op *AtomicWriteOperation) Delete(key string) keyvaluestore.AtomicWriteResult {
	return op.write(dynamodb.TransactWriteItem{
		Delete: &dynamodb.Delete{
			Key:       compositeKey(key, "_"),
			TableName: &op.Backend.TableName,
		},
	})
}

func (op *AtomicWriteOperation) Exec() (bool, error) {
	token := make([]byte, 20)
	if _, err := rand.Read(token); err != nil {
		return false, errors.Wrap(err, "unable to generate request token")
	}

	input := &dynamodb.TransactWriteItemsInput{
		TransactItems:      op.items,
		ClientRequestToken: aws.String(base64.RawURLEncoding.EncodeToString(token)),
	}

	attempts := 0
	for {
		_, err := op.Backend.Client.TransactWriteItems(input)
		if err == nil {
			return true, nil
		}

		if attempts < 3 && err.Code() == "InternalServerError" {
			// Internal errors tend to happen if the database was recently recreated. We should
			// retry the request a few times.
			attempts++
			time.Sleep(time.Duration(attempts*attempts) * 100 * time.Millisecond)
			continue
		}

		// The documentation says "TransactionCancelledException", but the API returns
		// "TransactionCanceledException"...
		if err.Code() != "TransactionCancelledException" && err.Code() != "TransactionCanceledException" {
			return false, err
		}

		hasErr := false
		hasConditionalCheckFailed := false
		for i, err := range err.CancellationReasons() {
			op.results[i].err = err
			if err != nil {
				if err.Code() != "ConditionalCheckFailed" {
					hasErr = true
				} else {
					hasConditionalCheckFailed = true
				}
			}
		}
		if hasErr || !hasConditionalCheckFailed {
			return false, err
		}

		return false, nil
	}
}
