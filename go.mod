module github.com/theaaf/keyvaluestore

require (
	github.com/antlr/antlr4 v0.0.0-20190325153624-837aa60e2c47 // indirect
	github.com/aws/aws-dax-go v0.0.0-00010101000000-000000000000
	github.com/aws/aws-sdk-go v1.19.7
	github.com/ccbrown/go-immutable v0.0.0-20171011001311-e9015daa17c4
	github.com/go-redis/redis v6.15.2+incompatible
	github.com/json-iterator/go v1.1.6
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.1 // indirect
	github.com/pkg/errors v0.8.1
	github.com/stretchr/testify v1.3.0
	golang.org/x/sync v0.0.0-20190227155943-e225da77a7e6
)

replace github.com/aws/aws-dax-go => github.com/theaaf/aws-dax-go v0.0.0-20190402210323-e66d6b079d7b
