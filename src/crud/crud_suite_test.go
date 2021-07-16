package crud_test

import (
	"github.com/geometry-labs/icon-blocks/config"
	"github.com/geometry-labs/icon-blocks/crud"
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var (
	transactionModelMongo *crud.TransactionModelMongo
)

func TestCrud(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Crud Suite")
}

var _ = BeforeSuite(func() {
	config.ReadEnvironment()
	//logging.StartLoggingInit()

	transactionModelMongo = NewTransactionModelMongo()
})

func NewTransactionModelMongo() *crud.TransactionModelMongo {
	testBlockRawModel := crud.GetTransactionModelMongo()
	return testBlockRawModel
}
