package dynamodbstore

import (
	"encoding/binary"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"

	"github.aaf.cloud/platform/keyvaluestore"
)

type batchedGet struct {
	value *string
	err   error
}

func (g batchedGet) Result() (*string, error) {
	return g.value, g.err
}

type batchedSMembers struct {
	members []string
	err     error
}

func (g batchedSMembers) Result() ([]string, error) {
	return g.members, g.err
}

type batchedWrite struct {
	request *dynamodb.WriteRequest
	err     error
}

func (w batchedWrite) Result() error {
	return w.err
}

type BatchOperation struct {
	*keyvaluestore.FallbackBatchOperation
	Backend *Backend

	gets      map[string]*batchedGet
	smemberss map[string]*batchedSMembers
	writes    map[string]*batchedWrite
}

func (op *BatchOperation) Get(key string) keyvaluestore.GetResult {
	if op.gets == nil {
		op.gets = make(map[string]*batchedGet)
	}
	if get, ok := op.gets[key]; ok {
		return get
	}
	get := &batchedGet{}
	op.gets[key] = get
	return get
}

func (op *BatchOperation) SMembers(key string) keyvaluestore.SMembersResult {
	if op.smemberss == nil {
		op.smemberss = make(map[string]*batchedSMembers)
	}
	if smembers, ok := op.smemberss[key]; ok {
		return smembers
	}
	smembers := &batchedSMembers{}
	op.smemberss[key] = smembers
	return smembers
}

func (op *BatchOperation) batchWrite(hashKey, rangeKey string, request *dynamodb.WriteRequest) keyvaluestore.ErrorResult {
	if op.writes == nil {
		op.writes = make(map[string]*batchedWrite)
	}

	var encodedHashKeyLength [8]byte
	binary.BigEndian.PutUint64(encodedHashKeyLength[:], uint64(len(hashKey)))
	mapKey := string(encodedHashKeyLength[:]) + hashKey + rangeKey

	if write, ok := op.writes[mapKey]; ok {
		write.request = request
		return write
	}
	write := &batchedWrite{
		request: request,
	}
	op.writes[mapKey] = write
	return write
}

func (op *BatchOperation) Set(key string, value interface{}) keyvaluestore.ErrorResult {
	return op.batchWrite(key, "_", &dynamodb.WriteRequest{
		PutRequest: &dynamodb.PutRequest{
			Item: newItem(key, "_", map[string]*dynamodb.AttributeValue{
				"v": attributeValue(value),
			}),
		},
	})
}

func (op *BatchOperation) ZAdd(key string, member interface{}, score float64) keyvaluestore.ErrorResult {
	s := *keyvaluestore.ToString(member)
	return op.batchWrite(key, s, &dynamodb.WriteRequest{
		PutRequest: &dynamodb.PutRequest{
			Item: newItem(key, s, map[string]*dynamodb.AttributeValue{
				"v":   attributeValue(s),
				"rk2": attributeValue(floatSortKey(score) + s),
			}),
		},
	})
}

func (op *BatchOperation) execReads() error {
	keys := make([]map[string]*dynamodb.AttributeValue, len(op.gets)+len(op.smemberss))
	i := 0
	for key := range op.gets {
		keys[i] = compositeKey(key, "_")
		i++
	}
	for key := range op.smemberss {
		keys[i] = setKey(key, 0)
		i++
	}

	if len(keys) == 0 {
		return nil
	}

	var g errgroup.Group

	for len(keys) > 0 {
		batch := keys
		const maxBatchSize = 100
		if len(batch) > maxBatchSize {
			batch = keys[:maxBatchSize]
		}
		keys = keys[len(batch):]

		g.Go(func() error {
			unprocessed := map[string]*dynamodb.KeysAndAttributes{
				op.Backend.TableName: &dynamodb.KeysAndAttributes{
					ConsistentRead: aws.Bool(!op.Backend.AllowEventuallyConsistentReads),
					Keys:           batch,
				},
			}

			var ret error

			for len(unprocessed) > 0 {
				result, err := op.Backend.Client.BatchGetItem(&dynamodb.BatchGetItemInput{
					RequestItems: unprocessed,
				})
				if err != nil {
					for _, key := range batch {
						key := *attributeStringValue(key["hk"])
						if get, ok := op.gets[key]; ok {
							get.err = err
						}
						if smembers, ok := op.smemberss[key]; ok {
							smembers.err = err
						}
					}
					return errors.Wrap(err, "dynamodb batch get item request error")
				}

				for _, item := range result.Responses[op.Backend.TableName] {
					key := *attributeStringValue(item["hk"])
					if get, ok := op.gets[key]; ok {
						get.value = attributeStringValue(item["v"])
					}
					if smembers, ok := op.smemberss[key]; ok {
						if item["c"].BOOL != nil && *item["c"].BOOL {
							smembers.members, smembers.err = op.Backend.SMembers(key)
							if smembers.err != nil {
								ret = smembers.err
							}
						} else {
							smembers.members = attributeStringSliceValue(item["v"])
						}
					}
				}

				unprocessed = result.UnprocessedKeys
			}

			return ret
		})
	}

	if err := g.Wait(); err != nil {
		return err
	}

	return nil
}

func (op *BatchOperation) execWrites() error {
	remainingWrites := make([]*batchedWrite, len(op.writes))
	i := 0
	for _, w := range op.writes {
		remainingWrites[i] = w
		i++
	}

	for len(remainingWrites) > 0 {
		batch := remainingWrites
		const maxBatchSize = 25
		if len(batch) > maxBatchSize {
			batch = remainingWrites[:maxBatchSize]
		}

		writeRequests := make([]*dynamodb.WriteRequest, len(batch))
		for i, w := range batch {
			writeRequests[i] = w.request
		}
		unprocessed := map[string][]*dynamodb.WriteRequest{
			op.Backend.TableName: writeRequests,
		}

		for len(unprocessed) > 0 {
			result, err := op.Backend.Client.BatchWriteItem(&dynamodb.BatchWriteItemInput{
				RequestItems: unprocessed,
			})
			if err != nil {
				for _, w := range remainingWrites {
					w.err = err
				}
				return errors.Wrap(err, "dynamodb batch write item request error")
			}
			unprocessed = result.UnprocessedItems
		}

		remainingWrites = remainingWrites[len(batch):]
	}

	return nil
}

func (op *BatchOperation) Exec() error {
	if err := op.execReads(); err != nil {
		return err
	} else if err := op.execWrites(); err != nil {
		return err
	}
	return op.FallbackBatchOperation.Exec()
}
