package crud_test

import (
	"github.com/geometry-labs/icon-blocks/config"
	"github.com/geometry-labs/icon-blocks/crud"
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var (
	blockModel *crud.TransactionModel
)

func TestCrud(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Crud Suite")
}

var _ = BeforeSuite(func() {
	config.ReadEnvironment()
	//logging.StartLoggingInit()

	blockModel = NewBlockModel()
	_ = blockModel.Migrate() // Have to create table before running tests
})

func NewBlockModel() *crud.TransactionModel {
	testBlockRawModel := crud.GetTransacstionModel()
	return testBlockRawModel
}
